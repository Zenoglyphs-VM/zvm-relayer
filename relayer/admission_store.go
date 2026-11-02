package relayer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	ErrAdmissionStoreLimit       = errors.New("admission retention limit reached")
	ErrAdmissionStoreUnavailable = errors.New("admission storage unavailable")
	ErrAdmissionStoreUnknown     = errors.New("admission storage outcome unknown")
)

// AdmissionStoreLimits bound retained records and exact signed bytes, including
// retired records. They are logical limits, not physical disk or OS-RSS quotas.
// Retired bytes are retained; there is no implicit pruning or repair operation.
type AdmissionStoreLimits struct {
	MaxRecords  int
	MaxRawBytes int64
}

func (l AdmissionStoreLimits) Checked() (AdmissionStoreLimits, error) {
	if l.MaxRecords == 0 {
		l.MaxRecords = 16384
	}
	if l.MaxRawBytes == 0 {
		l.MaxRawBytes = 64 << 20
	}
	if l.MaxRecords < 1 || l.MaxRecords > 262144 || l.MaxRawBytes < 1 || l.MaxRawBytes > 1<<30 {
		return l, errors.New("admission retention limits refused")
	}
	return l, nil
}

const (
	admissionActive byte = iota
	admissionRetired
)
const (
	admissionQueued byte = iota
	admissionPublishing
	admissionPosted
)
const (
	admissionNoRetirement byte = iota
	admissionObservedIncluded
	admissionNonceUsed
	admissionUnaffordable
	admissionRetriesExhausted
	admissionTTLExpired
	admissionReplaced
)

type admissionRecord struct {
	hash                 common.Hash
	raw                  []byte
	received, queuedAt   time.Time
	requeues             uint32
	shard                uint16
	state, reason, phase byte
}

type admissionRecordInfo struct {
	rawHash  [32]byte
	rawBytes int64
	state    byte
}

// AdmissionStore is an exclusively opened, domain-bound local store. Every
// accepted mutation requests a synchronous atomic database batch. This is a
// local storage contract; it is neither execution nor NoM finality evidence.
// Callers close it only after all Relayer operations have stopped.
type AdmissionStore struct {
	mu                        sync.Mutex
	db                        *leveldb.DB
	write                     func(*leveldb.Batch, *opt.WriteOptions) error
	root                      string
	identity                  os.FileInfo
	domain                    [32]byte
	signer                    types.Signer
	limits                    AdmissionStoreLimits
	maxTransactionBytes       int
	shards                    int
	items                     map[common.Hash]admissionRecordInfo
	rawBytes                  int64
	active                    int
	closed, poisoned, claimed bool
}

type AdmissionStoreStats struct {
	RetainedRecords  int   `json:"retainedRecords"`
	ActiveRecords    int   `json:"activeRecords"`
	RetainedRawBytes int64 `json:"retainedRawBytes"`
	MaxRecords       int   `json:"maxRecords"`
	MaxRawBytes      int64 `json:"maxRawBytes"`
	Healthy          bool  `json:"healthy"`
}

func admissionDomain(cfg Config) ([32]byte, error) {
	if cfg.Rollup == nil || len(cfg.Accounts) == 0 || len(cfg.Accounts) > 128 {
		return [32]byte{}, ErrAdmissionStoreUnavailable
	}
	addresses := make([]string, len(cfg.Accounts))
	seen := map[string]bool{}
	for i, account := range cfg.Accounts {
		if account == nil {
			return [32]byte{}, ErrAdmissionStoreUnavailable
		}
		addresses[i] = account.Address().String()
		if seen[addresses[i]] {
			return [32]byte{}, ErrAdmissionStoreUnavailable
		}
		seen[addresses[i]] = true
	}
	// Hash the complete rollup configuration and ordered posting accounts. Do
	// not include endpoint URLs, filesystem paths, private keys or credentials.
	raw, err := json.Marshal(struct {
		Version  int
		Rollup   any
		Accounts []string
	}{1, cfg.Rollup, addresses})
	if err != nil {
		return [32]byte{}, ErrAdmissionStoreUnavailable
	}
	return sha256.Sum256(raw), nil
}

func syncAdmissionDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// OpenAdmissionStore creates a new private directory or reopens its existing
// domain-bound database. Missing/corrupt state in an existing directory is not
// adopted or repaired. An interrupted initialization is preserved and refused.
func OpenAdmissionStore(path string, cfg Config, limits AdmissionStoreLimits) (*AdmissionStore, error) {
	limits, err := limits.Checked()
	if err != nil {
		return nil, err
	}
	domain, err := admissionDomain(cfg)
	if err != nil || cfg.Rollup.MaxAssembledBytes < 3 || cfg.Rollup.MaxAssembledBytes > 64<<20 {
		return nil, ErrAdmissionStoreUnavailable
	}
	root, err := filepath.Abs(path)
	if err != nil || path == "" || root == filepath.Dir(root) {
		return nil, ErrAdmissionStoreUnavailable
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(root))
	if err != nil || parent != filepath.Dir(root) {
		return nil, ErrAdmissionStoreUnavailable
	}
	info, err := os.Lstat(root)
	fresh := os.IsNotExist(err)
	if fresh {
		if err = os.Mkdir(root, 0700); err != nil {
			return nil, ErrAdmissionStoreUnavailable
		}
		info, err = os.Lstat(root)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, ErrAdmissionStoreUnavailable
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, ErrAdmissionStoreUnavailable
	}
	owner := []byte("ZVM admission owner v1\n" + hex.EncodeToString(domain[:]) + "\n")
	if !fresh {
		if len(entries) != 2 {
			return nil, ErrAdmissionStoreUnavailable
		}
		ownerPath := filepath.Join(root, "owner-v1")
		ownerInfo, err := os.Lstat(ownerPath)
		if err != nil || !ownerInfo.Mode().IsRegular() || ownerInfo.Mode().Perm()&0077 != 0 || ownerInfo.Size() != int64(len(owner)) {
			return nil, ErrAdmissionStoreUnavailable
		}
		saved, err := os.ReadFile(ownerPath)
		if err != nil || !bytes.Equal(saved, owner) {
			return nil, ErrAdmissionStoreUnavailable
		}
	}
	for _, entry := range entries {
		if entry.Name() == "owner-v1" {
			continue
		}
		if entry.Name() != "data" || !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil, ErrAdmissionStoreUnavailable
		}
	}
	if fresh {
		f, err := os.OpenFile(filepath.Join(root, "owner-v1"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, ErrAdmissionStoreUnavailable
		}
		n, writeErr := f.Write(owner)
		syncErr := f.Sync()
		closeErr := f.Close()
		if writeErr != nil || n != len(owner) || syncErr != nil || closeErr != nil || syncAdmissionDirectory(root) != nil {
			return nil, ErrAdmissionStoreUnavailable
		}
	}
	db, err := leveldb.OpenFile(filepath.Join(root, "data"), &opt.Options{
		ErrorIfExist: fresh, ErrorIfMissing: !fresh, Strict: opt.StrictAll,
		NoSync: false, BlockCacheCapacity: 4 << 20, WriteBuffer: 2 << 20, OpenFilesCacheCapacity: 32,
	})
	if err != nil {
		return nil, ErrAdmissionStoreUnavailable
	}
	s := &AdmissionStore{db: db, write: db.Write, root: root, identity: info, domain: domain,
		signer: types.LatestSignerForChainID(cfg.Rollup.L2ChainIDBig()), limits: limits,
		maxTransactionBytes: cfg.Rollup.MaxAssembledBytes - 2, shards: len(cfg.Accounts),
		items: map[common.Hash]admissionRecordInfo{}}
	failed := true
	defer func() {
		if failed {
			db.Close()
		}
	}()
	if fresh {
		batch := new(leveldb.Batch)
		batch.Put([]byte("domain-v1"), domain[:])
		if db.Write(batch, &opt.WriteOptions{Sync: true}) != nil {
			return nil, ErrAdmissionStoreUnknown
		}
	} else {
		saved, err := db.Get([]byte("domain-v1"), nil)
		if err != nil || !bytes.Equal(saved, domain[:]) {
			return nil, ErrAdmissionStoreUnavailable
		}
	}
	if err := s.readAllLocked(); err != nil {
		return nil, err
	}
	if syncAdmissionDirectory(filepath.Join(root, "data")) != nil || syncAdmissionDirectory(root) != nil || syncAdmissionDirectory(filepath.Dir(root)) != nil {
		return nil, ErrAdmissionStoreUnavailable
	}
	failed = false
	return s, nil
}

func admissionKey(hash common.Hash) []byte { return append([]byte("t/"), hash[:]...) }

// A versioned binary frame preserves exact bytes without base64 expansion.
// Its checksum detects corruption; it is not an authentication or consensus proof.
func (s *AdmissionStore) validateRecord(record admissionRecord) error {
	if len(record.raw) == 0 || len(record.raw) > s.maxTransactionBytes ||
		record.received.UnixNano() <= 0 || record.queuedAt.UnixNano() <= 0 || int(record.shard) >= s.shards ||
		record.state > admissionRetired || record.reason > admissionReplaced ||
		record.phase > admissionPosted ||
		(record.state == admissionActive) != (record.reason == admissionNoRetirement) {
		return ErrAdmissionStoreUnavailable
	}
	tx := new(types.Transaction)
	if tx.UnmarshalBinary(record.raw) != nil || tx.Hash() != record.hash {
		return ErrAdmissionStoreUnavailable
	}
	if _, err := types.Sender(s.signer, tx); err != nil {
		return ErrAdmissionStoreUnavailable
	}
	return nil
}

func (s *AdmissionStore) encode(record admissionRecord) ([]byte, error) {
	if err := s.validateRecord(record); err != nil {
		return nil, err
	}
	raw := make([]byte, 37+len(record.raw)+sha256.Size)
	copy(raw, "ZVMADM01")
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(record.raw)))
	binary.BigEndian.PutUint64(raw[12:20], uint64(record.received.UnixNano()))
	binary.BigEndian.PutUint64(raw[20:28], uint64(record.queuedAt.UnixNano()))
	binary.BigEndian.PutUint32(raw[28:32], record.requeues)
	binary.BigEndian.PutUint16(raw[32:34], record.shard)
	raw[34], raw[35] = record.state, record.reason
	raw[36] = record.phase
	copy(raw[37:], record.raw)
	h := sha256.New()
	h.Write(s.domain[:])
	h.Write(record.hash[:])
	h.Write(raw[:len(raw)-sha256.Size])
	copy(raw[len(raw)-sha256.Size:], h.Sum(nil))
	return raw, nil
}

func (s *AdmissionStore) decode(hash common.Hash, raw []byte) (admissionRecord, error) {
	var record admissionRecord
	if len(raw) < 37+1+sha256.Size || string(raw[:8]) != "ZVMADM01" {
		return record, ErrAdmissionStoreUnavailable
	}
	length := int64(binary.BigEndian.Uint32(raw[8:12]))
	if length > int64(s.maxTransactionBytes) || length != int64(len(raw)-37-sha256.Size) {
		return record, ErrAdmissionStoreUnavailable
	}
	h := sha256.New()
	h.Write(s.domain[:])
	h.Write(hash[:])
	h.Write(raw[:len(raw)-sha256.Size])
	if !bytes.Equal(h.Sum(nil), raw[len(raw)-sha256.Size:]) {
		return record, ErrAdmissionStoreUnavailable
	}
	record = admissionRecord{hash: hash, raw: bytes.Clone(raw[37 : len(raw)-sha256.Size]),
		received: time.Unix(0, int64(binary.BigEndian.Uint64(raw[12:20]))),
		queuedAt: time.Unix(0, int64(binary.BigEndian.Uint64(raw[20:28]))),
		requeues: binary.BigEndian.Uint32(raw[28:32]), shard: binary.BigEndian.Uint16(raw[32:34]), state: raw[34], reason: raw[35], phase: raw[36]}
	if err := s.validateRecord(record); err != nil {
		return admissionRecord{}, err
	}
	return record, nil
}

func (s *AdmissionStore) identityOK() bool {
	info, err := os.Lstat(s.root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || !os.SameFile(info, s.identity) {
		return false
	}
	resolved, err := filepath.EvalSymlinks(s.root)
	return err == nil && resolved == s.root
}

func (s *AdmissionStore) readAllLocked() error {
	if s.closed || s.poisoned || !s.identityOK() {
		return ErrAdmissionStoreUnavailable
	}
	iter := s.db.NewIterator(nil, &opt.ReadOptions{Strict: opt.StrictAll})
	defer iter.Release()
	items := map[common.Hash]admissionRecordInfo{}
	var total int64
	active := 0
	domainSeen := false
	for iter.Next() {
		key, value := iter.Key(), iter.Value()
		if bytes.Equal(key, []byte("domain-v1")) {
			if domainSeen || !bytes.Equal(value, s.domain[:]) {
				return ErrAdmissionStoreUnavailable
			}
			domainSeen = true
			continue
		}
		if len(key) != 34 || !bytes.HasPrefix(key, []byte("t/")) || len(items) == s.limits.MaxRecords {
			return ErrAdmissionStoreUnavailable
		}
		if int64(len(value)) > s.limits.MaxRawBytes-total+37+sha256.Size {
			return ErrAdmissionStoreUnavailable
		}
		hash := common.BytesToHash(key[2:])
		record, err := s.decode(hash, value)
		if err != nil || int64(len(record.raw)) > s.limits.MaxRawBytes-total {
			return ErrAdmissionStoreUnavailable
		}
		total += int64(len(record.raw))
		items[hash] = admissionRecordInfo{sha256.Sum256(record.raw), int64(len(record.raw)), record.state}
		if record.state == admissionActive {
			active++
		}
	}
	if iter.Error() != nil || !domainSeen {
		return ErrAdmissionStoreUnavailable
	}
	s.items, s.rawBytes, s.active = items, total, active
	return nil
}

func (s *AdmissionStore) healthy() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed && !s.poisoned && s.identityOK()
}

func (s *AdmissionStore) records() ([]admissionRecord, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned || !s.identityOK() {
		return nil, ErrAdmissionStoreUnavailable
	}
	var hashes []common.Hash
	for hash, info := range s.items {
		if info.state == admissionActive {
			hashes = append(hashes, hash)
		}
	}
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	var records []admissionRecord
	for _, hash := range hashes {
		raw, err := s.db.Get(admissionKey(hash), &opt.ReadOptions{Strict: opt.StrictAll})
		if err != nil {
			s.poisoned = true
			return nil, ErrAdmissionStoreUnavailable
		}
		record, err := s.decode(hash, raw)
		info := s.items[hash]
		if err != nil || record.state != admissionActive || int64(len(record.raw)) != info.rawBytes || sha256.Sum256(record.raw) != info.rawHash {
			s.poisoned = true
			return nil, ErrAdmissionStoreUnavailable
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *AdmissionStore) attach(cfg Config) error {
	if s == nil {
		return nil
	}
	domain, err := admissionDomain(cfg)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned || s.claimed || domain != s.domain || !s.identityOK() {
		return ErrAdmissionStoreUnavailable
	}
	s.claimed = true
	return nil
}

// apply changes all supplied lifecycle records atomically. A known retention
// refusal happens before writing. An I/O failure after starting Write has an
// UNKNOWN outcome: poison this instance and require a verified reopen.
func (s *AdmissionStore) apply(records []admissionRecord) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poisoned || !s.identityOK() {
		return ErrAdmissionStoreUnavailable
	}
	batch := new(leveldb.Batch)
	updates := make(map[common.Hash]admissionRecordInfo, len(records))
	count, total, active := len(s.items), s.rawBytes, s.active
	for _, record := range records {
		if _, duplicate := updates[record.hash]; duplicate {
			return ErrAdmissionStoreUnavailable
		}
		info := admissionRecordInfo{sha256.Sum256(record.raw), int64(len(record.raw)), record.state}
		if previous, exists := s.items[record.hash]; exists {
			if previous.rawHash != info.rawHash || previous.rawBytes != info.rawBytes {
				return ErrAdmissionStoreUnavailable
			}
			if previous.state == admissionActive {
				active--
			}
		} else {
			count++
			total += info.rawBytes
		}
		if info.state == admissionActive {
			active++
		}
		if count > s.limits.MaxRecords || total > s.limits.MaxRawBytes {
			return ErrAdmissionStoreLimit
		}
		raw, err := s.encode(record)
		if err != nil {
			return err
		}
		updates[record.hash] = info
		batch.Put(admissionKey(record.hash), raw)
	}
	if len(records) == 0 {
		return nil
	}
	if s.write(batch, &opt.WriteOptions{Sync: true}) != nil {
		s.poisoned = true
		return ErrAdmissionStoreUnknown
	}
	for hash, info := range updates {
		s.items[hash] = info
	}
	s.rawBytes, s.active = total, active
	return nil
}

func (s *AdmissionStore) Stats() AdmissionStoreStats {
	if s == nil {
		return AdmissionStoreStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := AdmissionStoreStats{RetainedRecords: len(s.items), ActiveRecords: s.active, RetainedRawBytes: s.rawBytes,
		MaxRecords: s.limits.MaxRecords, MaxRawBytes: s.limits.MaxRawBytes, Healthy: !s.closed && !s.poisoned && s.identityOK()}
	return stats
}

func (s *AdmissionStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.db == nil {
		return ErrAdmissionStoreUnavailable
	}
	if s.db.Close() != nil {
		return ErrAdmissionStoreUnavailable
	}
	return nil
}
