package relayer

import (
	"bytes"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var ErrAdmissionPublicationUnknown = errors.New("durable admission publication outcome unresolved")

func (r *Relayer) admissionStatus() (string, bool, *AdmissionStoreStats) {
	if r.cfg.AdmissionStore == nil {
		return "volatile", false, nil
	}
	stats := r.cfg.AdmissionStore.Stats()
	if !stats.Healthy {
		r.failAdmissionStore(ErrAdmissionStoreUnavailable)
		return "local-store-failed", false, &stats
	}
	return "local-sync", true, &stats
}

func admissionFromEntry(e *entry, shard int, reason byte, phase byte) (admissionRecord, error) {
	if e == nil || shard < 0 || shard > math.MaxUint16 || e.requeues < 0 || uint64(e.requeues) > math.MaxUint32 {
		return admissionRecord{}, ErrAdmissionStoreUnavailable
	}
	state := admissionActive
	if reason != admissionNoRetirement {
		state = admissionRetired
	}
	return admissionRecord{hash: e.tx.Hash(), raw: e.raw, received: e.received, queuedAt: e.queuedAt,
		requeues: uint32(e.requeues), shard: uint16(shard), state: state, reason: reason, phase: phase}, nil
}

func (r *Relayer) failAdmissionStore(err error) {
	if r.cfg.AdmissionStore == nil {
		return
	}
	r.admissionFailureOnce.Do(func() {
		r.admissionStoreError = err
		r.admissionReady.Store(false)
		r.recoveryPhase.Store(2)
		close(r.admissionFailure)
	})
}

func (r *Relayer) admissionStoreOperational() bool {
	if r.cfg.AdmissionStore == nil {
		return true
	}
	select {
	case <-r.admissionFailure:
		return false
	default:
	}
	if !r.cfg.AdmissionStore.healthy() {
		r.failAdmissionStore(ErrAdmissionStoreUnavailable)
		return false
	}
	return true
}

// Callers hold r.mu while applying lifecycle records. In-memory state changes
// follow the synchronous batch. A failed write stops admission and publishing;
// its exact bytes and UNKNOWN outcome must be reconciled on verified reopen.
func (r *Relayer) persistAdmissions(records []admissionRecord) error {
	if r.cfg.AdmissionStore == nil {
		return nil
	}
	if !r.admissionStoreOperational() {
		return ErrAdmissionStoreUnavailable
	}
	err := r.cfg.AdmissionStore.apply(records)
	if err != nil && !errors.Is(err, ErrAdmissionStoreLimit) {
		r.failAdmissionStore(err)
	}
	return err
}

func (r *Relayer) persistNewAdmission(e, previous *entry, shard int) error {
	if r.cfg.AdmissionStore == nil {
		return nil
	}
	var records []admissionRecord
	if previous != nil {
		record, err := admissionFromEntry(previous, shard, admissionReplaced, admissionQueued)
		if err != nil {
			r.failAdmissionStore(err)
			return err
		}
		records = append(records, record)
	}
	record, err := admissionFromEntry(e, shard, admissionNoRetirement, admissionQueued)
	if err != nil {
		r.failAdmissionStore(err)
		return err
	}
	records = append(records, record)
	if err := r.persistAdmissions(records); errors.Is(err, ErrAdmissionStoreLimit) {
		return reject("durable admission retention limit reached")
	} else {
		return err
	}
}

func (r *Relayer) releasePublication(entries []*entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range entries {
		e.publishing = false
	}
}

// reservePublication is called after encoding but before any NoM write. The
// selected objects were reserved under r.mu, so replacement and TTL expiry
// cannot remove the signed bytes while the external operation is in progress.
func (r *Relayer) reservePublication(candidates, selected []*entry, shard int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	keep := map[*entry]bool{}
	var records []admissionRecord
	for _, e := range selected {
		if e == nil || !e.publishing || r.byHash[e.tx.Hash()] != e || r.senders[e.from].queued[e.tx.Nonce()] != e {
			return ErrAdmissionStoreUnavailable
		}
		keep[e] = true
		if r.cfg.AdmissionStore != nil {
			record, err := admissionFromEntry(e, shard, admissionNoRetirement, admissionPublishing)
			if err != nil {
				r.failAdmissionStore(err)
				return err
			}
			records = append(records, record)
		}
	}
	for _, e := range candidates {
		if !keep[e] {
			e.publishing = false
		}
	}
	return r.persistAdmissions(records)
}

func (r *Relayer) persistPosted(entries []*entry, shard int) error {
	if r.cfg.AdmissionStore == nil {
		return nil
	}
	var records []admissionRecord
	for _, e := range entries {
		record, err := admissionFromEntry(e, shard, admissionNoRetirement, admissionPosted)
		if err != nil {
			r.failAdmissionStore(err)
			return err
		}
		records = append(records, record)
	}
	return r.persistAdmissions(records)
}

func (r *Relayer) persistSettlement(b *batch, included map[common.Hash]bool, nonces map[common.Address]uint64, unaffordable map[common.Hash]bool, now time.Time) error {
	if r.cfg.AdmissionStore == nil {
		return nil
	}
	var records []admissionRecord
	for _, e := range b.entries {
		reason, phase := byte(admissionNoRetirement), byte(admissionQueued)
		copy := *e
		if included[e.tx.Hash()] {
			reason, phase = admissionObservedIncluded, admissionPosted
		} else if unaffordable[e.tx.Hash()] {
			reason, phase = admissionUnaffordable, admissionPosted
		} else {
			chainNonce := nonces[e.from]
			if e.tx.Nonce() <= chainNonce {
				copy.requeues++
			}
			_, keep := requeuePlan(e.tx.Nonce(), chainNonce, r.senders[e.from].next, copy.requeues, r.cfg.MaxRequeues)
			if !keep {
				phase = admissionPosted
				if e.tx.Nonce() < chainNonce {
					reason = admissionNonceUsed
				} else {
					reason = admissionRetriesExhausted
				}
			} else {
				copy.queuedAt = now
			}
		}
		record, err := admissionFromEntry(&copy, b.shard, reason, phase)
		if err != nil {
			r.failAdmissionStore(err)
			return err
		}
		records = append(records, record)
	}
	return r.persistAdmissions(records)
}

func (r *Relayer) persistSweep(now time.Time) error {
	if r.cfg.AdmissionStore == nil {
		return nil
	}
	var records []admissionRecord
	for _, s := range r.senders {
		live := s.reachable()
		for nonce, e := range s.queued {
			if e.publishing || live[nonce] || now.Sub(e.queuedAt) <= r.cfg.QueueTTL {
				continue
			}
			record, err := admissionFromEntry(e, s.shard, admissionTTLExpired, admissionQueued)
			if err != nil {
				r.failAdmissionStore(err)
				return err
			}
			records = append(records, record)
		}
	}
	return r.persistAdmissions(records)
}

func (r *Relayer) admissionTransaction(record admissionRecord) (*types.Transaction, common.Address, error) {
	tx := new(types.Transaction)
	if tx.UnmarshalBinary(record.raw) != nil || tx.Hash() != record.hash ||
		tx.Protected() && tx.ChainId().Cmp(r.cfg.Rollup.L2ChainIDBig()) != 0 {
		return nil, common.Address{}, ErrAdmissionStoreUnavailable
	}
	from, err := types.Sender(r.signer, tx)
	if err != nil {
		return nil, common.Address{}, ErrAdmissionStoreUnavailable
	}
	return tx, from, nil
}

// mergeAdmissionRecovery changes only staged maps. An unresolved publication or
// conflicting nonce cannot be silently requeued. An exact posted hash found in
// complete source history wins its actual posting shard without another write.
func (r *Relayer) mergeAdmissionRecovery(staged *Relayer, journal []admissionRecord, nonces map[common.Address]uint64, now time.Time) ([]admissionRecord, error) {
	if r.cfg.AdmissionStore == nil {
		return nil, nil
	}
	limits, err := r.cfg.Limits.checked()
	if err != nil {
		return nil, err
	}
	var retired []admissionRecord
	for _, record := range journal {
		tx, from, err := r.admissionTransaction(record)
		if err != nil {
			return nil, err
		}
		floor, known := nonces[from]
		if !known {
			return nil, ErrAdmissionStoreUnavailable
		}
		if tx.Nonce() < floor {
			record.state, record.reason = admissionRetired, admissionNonceUsed
			retired = append(retired, record)
			continue
		}
		if existing := staged.byHash[record.hash]; existing != nil {
			if !bytes.Equal(existing.raw, record.raw) || existing.from != from {
				return nil, ErrAdmissionStoreUnavailable
			}
			existing.received, existing.queuedAt, existing.requeues = record.received, record.queuedAt, int(record.requeues)
			continue
		}
		if record.phase != admissionQueued {
			return nil, ErrAdmissionPublicationUnknown
		}
		s := staged.senders[from]
		if s != nil && (s.queued[tx.Nonce()] != nil || s.posted[tx.Nonce()] != nil) {
			return nil, ErrAdmissionPublicationUnknown
		}
		if err := staged.checkCapacity(limits, from, len(record.raw), nil); err != nil {
			return nil, err
		}
		if s == nil {
			if int(record.shard) >= len(staged.byShard) {
				return nil, ErrAdmissionStoreUnavailable
			}
			s = &sender{addr: from, shard: int(record.shard), next: floor, queued: map[uint64]*entry{}, posted: map[uint64]*entry{}}
			staged.senders[from] = s
			staged.byShard[s.shard][from] = s
		}
		if s.queued[tx.Nonce()] != nil || s.posted[tx.Nonce()] != nil {
			return nil, ErrAdmissionPublicationUnknown
		}
		e := &entry{tx: tx, raw: bytes.Clone(record.raw), from: from, received: record.received, queuedAt: record.queuedAt, requeues: int(record.requeues)}
		s.queued[tx.Nonce()] = e
		s.lastUse = now
		staged.remember(e)
		staged.nQueued++
		staged.load[s.shard]++
	}
	for _, s := range staged.senders {
		s.settleCursor(0)
	}
	var hashes []common.Hash
	for hash := range staged.byHash {
		hashes = append(hashes, hash)
	}
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	records := retired
	for _, hash := range hashes {
		e := staged.byHash[hash]
		phase := byte(admissionQueued)
		if e.state == posted {
			phase = admissionPosted
		}
		record, err := admissionFromEntry(e, staged.senders[e.from].shard, admissionNoRetirement, phase)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}
