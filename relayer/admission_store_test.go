package relayer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func storedRecoveryRelayer(t *testing.T, f *recoveryFixture, path string, limits AdmissionStoreLimits) (*Relayer, *AdmissionStore) {
	t.Helper()
	r := New(f.relayer().cfg)
	store, err := OpenAdmissionStore(path, r.cfg, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	r.cfg.AdmissionStore = store
	return r, store
}

func activateStoredRecovery(t *testing.T, r *Relayer) {
	t.Helper()
	if err := r.cfg.AdmissionStore.attach(r.cfg); err != nil {
		t.Fatal(err)
	}
	if err := r.recoverInFlight(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.recoveryPhase.Store(1)
	r.admissionReady.Store(true)
	close(r.adopted)
}

func signedAdmissionHash(t *testing.T, raw []byte) common.Hash {
	t.Helper()
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(raw); err != nil {
		t.Fatal(err)
	}
	return tx.Hash()
}

func retainedAdmission(t *testing.T, store *AdmissionStore, hash common.Hash) admissionRecord {
	t.Helper()
	raw, err := store.db.Get(admissionKey(hash), nil)
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.decode(hash, raw)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestAdmissionStoreCheckedRetentionLimits(t *testing.T) {
	defaults, err := (AdmissionStoreLimits{}).Checked()
	if err != nil || defaults.MaxRecords != 16384 || defaults.MaxRawBytes != 64<<20 {
		t.Fatal("unexpected defaults")
	}
	for _, limits := range []AdmissionStoreLimits{{MaxRecords: -1}, {MaxRecords: 262145}, {MaxRawBytes: -1}, {MaxRawBytes: (1 << 30) + 1}} {
		if _, err := limits.Checked(); err == nil {
			t.Fatal("invalid retention limit accepted")
		}
	}
}

func TestAdmissionStorePrivateOwnershipAndDomain(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	if _, err := OpenAdmissionStore(path, r.cfg, AdmissionStoreLimits{}); err == nil {
		t.Fatal("second writer accepted")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	changed := r.cfg
	rollup := *r.cfg.Rollup
	rollup.L2ChainID++
	changed.Rollup = &rollup
	if _, err := OpenAdmissionStore(path, changed, AdmissionStoreLimits{}); err == nil {
		t.Fatal("foreign domain adopted")
	}
	store, err := OpenAdmissionStore(path, r.cfg, AdmissionStoreLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if !store.Stats().Healthy {
		t.Fatal("valid original domain did not reopen")
	}
	empty := filepath.Join(t.TempDir(), "unowned")
	if err := os.Mkdir(empty, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAdmissionStore(empty, r.cfg, AdmissionStoreLimits{}); err == nil {
		t.Fatal("unowned existing directory adopted")
	}
	entries, _ := os.ReadDir(empty)
	if len(entries) != 0 {
		t.Fatal("refusal modified unowned directory")
	}
}

func TestAdmissionStoreRefusesLinksAndPublicPermissions(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	cfg := f.relayer().cfg
	parent := t.TempDir()
	target := filepath.Join(parent, "target")
	store, err := OpenAdmissionStore(target, cfg, AdmissionStoreLimits{})
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	link := filepath.Join(parent, "alias")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAdmissionStore(link, cfg, AdmissionStoreLimits{}); err == nil {
		t.Fatal("linked store adopted")
	}
	if err := os.Chmod(target, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAdmissionStore(target, cfg, AdmissionStoreLimits{}); err == nil {
		t.Fatal("publicly accessible store accepted")
	}
}

func TestDurableAdmissionOwnsBytesAndReopensQueuedHash(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	raw := bytes.Clone(f.raws[0])
	original := bytes.Clone(raw)
	hash := signedAdmissionHash(t, raw)
	if got, err := r.Submit(context.Background(), raw); err != nil || got != hash {
		t.Fatal("original admission failed", err)
	}
	raw[0] ^= 0xff
	if !bytes.Equal(r.byHash[hash].raw, original) || !bytes.Equal(retainedAdmission(t, store, hash).raw, original) {
		t.Fatal("caller mutation changed owned bytes")
	}
	if got, err := r.Submit(context.Background(), original); err != nil || got != hash || store.Stats().RetainedRecords != 1 {
		t.Fatal("duplicate admission changed retained identity")
	}
	store.Close()
	reopened, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, reopened)
	if reopened.nQueued != 1 || reopened.nPosted != 0 || !bytes.Equal(reopened.byHash[hash].raw, original) {
		t.Fatal("queued bytes were not restored")
	}
	if got, err := reopened.Submit(context.Background(), original); err != nil || got != hash {
		t.Fatal("original hash did not remain idempotent")
	}
	if nextStore.Stats().RetainedRecords != 1 || f.writes.Load() != 0 {
		t.Fatal("recovery wrote to NoM or duplicated retention")
	}
}

func TestDurableAdmissionReplacementRetainsBothExactPayloads(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	a := f.raws[0]
	ah := signedAdmissionHash(t, a)
	b := send(t, f.key, 0, zvm(1), 5)
	bh := signedAdmissionHash(t, b)
	if _, err := r.Submit(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Submit(context.Background(), b); err != nil || got != bh {
		t.Fatal(err)
	}
	if r.byHash[ah] != nil || r.byHash[bh] == nil || r.nQueued != 1 {
		t.Fatal("replacement live state disagrees")
	}
	old := retainedAdmission(t, store, ah)
	next := retainedAdmission(t, store, bh)
	if old.state != admissionRetired || old.reason != admissionReplaced || !bytes.Equal(old.raw, a) || next.state != admissionActive || !bytes.Equal(next.raw, b) {
		t.Fatal("replacement did not retain exact atomic lifecycle")
	}
	store.Close()
	reopened, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, reopened)
	if reopened.byHash[ah] != nil || reopened.byHash[bh] == nil || nextStore.Stats().RetainedRecords != 2 || nextStore.Stats().ActiveRecords != 1 {
		t.Fatal("retired replacement resurrected")
	}
}

func TestDurableAdmissionLimitRefusalLeavesOriginalActive(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	r, store := storedRecoveryRelayer(t, f, filepath.Join(t.TempDir(), "admissions"), AdmissionStoreLimits{MaxRecords: 1})
	activateStoredRecovery(t, r)
	a := f.raws[0]
	ah := signedAdmissionHash(t, a)
	b := send(t, f.key, 0, zvm(1), 5)
	if _, err := r.Submit(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit(context.Background(), b); err == nil {
		t.Fatal("retention overflow acknowledged")
	}
	if !r.Ready() || r.byHash[ah] == nil || r.nQueued != 1 || retainedAdmission(t, store, ah).state != admissionActive {
		t.Fatal("known refusal changed original state")
	}
}

func TestDurableAdmissionAmbiguousWriteStopsWithoutFalseAcknowledgement(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	a := f.raws[0]
	ah := signedAdmissionHash(t, a)
	b := send(t, f.key, 0, zvm(1), 5)
	bh := signedAdmissionHash(t, b)
	if _, err := r.Submit(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	original := store.write
	store.write = func(batch *leveldb.Batch, options *opt.WriteOptions) error {
		if options == nil || !options.Sync {
			t.Fatal("write omitted synchronous barrier")
		}
		if err := original(batch, options); err != nil {
			return err
		}
		return errors.New("private backend diagnostic")
	}
	if _, err := r.Submit(context.Background(), b); !errors.Is(err, ErrAdmissionStoreUnknown) || strings.Contains(err.Error(), "private") {
		t.Fatal("ambiguous write did not return a safe UNKNOWN error")
	}
	if r.Ready() || r.byHash[ah] == nil || r.byHash[bh] != nil || store.Stats().Healthy {
		t.Fatal("ambiguous write changed live state or remained healthy")
	}
	store.Close()
	reopened, _ := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, reopened)
	if reopened.byHash[ah] != nil || reopened.byHash[bh] == nil || !bytes.Equal(reopened.byHash[bh].raw, b) {
		t.Fatal("verified reopen did not resolve the actual atomic write")
	}
}

func TestDurablePublicationReservationRefusesReplacementAndUnknownReplay(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	a := f.raws[0]
	ah := signedAdmissionHash(t, a)
	if _, err := r.Submit(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	e := r.byHash[ah]
	r.mu.Lock()
	e.publishing = true
	r.mu.Unlock()
	if err := r.reservePublication([]*entry{e}, []*entry{e}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit(context.Background(), send(t, f.key, 0, zvm(1), 5)); err == nil {
		t.Fatal("publication reservation was replaced")
	}
	if retainedAdmission(t, store, ah).phase != admissionPublishing || r.byHash[ah] != e {
		t.Fatal("publication intent was not retained")
	}
	store.Close()
	reopened, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	if err := reopened.recoverInFlight(context.Background()); !errors.Is(err, ErrAdmissionPublicationUnknown) {
		t.Fatal("unresolved publication silently requeued", err)
	}
	if len(reopened.byHash) != 0 || reopened.Ready() || f.writes.Load() != 0 || retainedAdmission(t, nextStore, ah).phase != admissionPublishing {
		t.Fatal("UNKNOWN replay changed live state or published")
	}
}

func TestDurablePostedRecoveryUsesCompleteOriginalHash(t *testing.T) {
	f := recoveryFixtureFor(t, 130, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	hash := signedAdmissionHash(t, f.raws[0])
	if r.nPosted != 1 || r.nQueued != 0 || retainedAdmission(t, store, hash).phase != admissionPosted {
		t.Fatal("posted source was not durably adopted")
	}
	if got, err := r.Submit(context.Background(), f.raws[0]); err != nil || got != hash {
		t.Fatal("posted duplicate changed identity")
	}
	store.Close()
	reopened, _ := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, reopened)
	if reopened.nPosted != 1 || reopened.nQueued != 0 || f.writes.Load() != 0 || f.pages.Load() < 6 {
		t.Fatal("posted replay skipped complete history or republished")
	}
}

func TestDurableTTLRetirementDoesNotResurrectBytes(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	raw := send(t, f.key, 9, zvm(1), 1)
	hash := signedAdmissionHash(t, raw)
	if _, err := r.Submit(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	r.byHash[hash].queuedAt = time.Now().Add(-2 * r.cfg.QueueTTL)
	r.sweep()
	record := retainedAdmission(t, store, hash)
	if r.byHash[hash] != nil || record.state != admissionRetired || record.reason != admissionTTLExpired || !bytes.Equal(record.raw, raw) {
		t.Fatal("TTL retirement lost bytes or remained active")
	}
	store.Close()
	reopened, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, reopened)
	if reopened.nQueued != 0 || nextStore.Stats().RetainedRecords != 1 || nextStore.Stats().ActiveRecords != 0 {
		t.Fatal("TTL retirement resurrected")
	}
}

func TestDurableCanonicalNonceRetiresWithoutDeletingBytes(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	raw := f.raws[0]
	hash := signedAdmissionHash(t, raw)
	if _, err := r.Submit(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	store.Close()
	f.nonce = 1
	reopened, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, reopened)
	record := retainedAdmission(t, nextStore, hash)
	if reopened.nQueued != 0 || record.state != admissionRetired || record.reason != admissionNonceUsed || !bytes.Equal(record.raw, raw) {
		t.Fatal("canonical nonce retirement lost original bytes")
	}
}

func TestDurableRecoveryRefusalPreservesStoreAndLiveState(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	raw := f.raws[0]
	hash := signedAdmissionHash(t, raw)
	if _, err := r.Submit(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	store.Close()
	f.mode = "short_page"
	reopened, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	if err := reopened.recoverInFlight(context.Background()); err == nil {
		t.Fatal("incomplete history accepted")
	}
	if len(reopened.byHash) != 0 || reopened.Ready() || retainedAdmission(t, nextStore, hash).state != admissionActive || f.writes.Load() != 0 {
		t.Fatal("failed recovery changed staged/live durability state")
	}
}

func TestAdmissionStoreFrameCorruptionAndStateRefusals(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	r, store := storedRecoveryRelayer(t, f, filepath.Join(t.TempDir(), "admissions"), AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	if _, err := r.Submit(context.Background(), f.raws[0]); err != nil {
		t.Fatal(err)
	}
	hash := signedAdmissionHash(t, f.raws[0])
	original, _ := store.db.Get(admissionKey(hash), nil)
	for _, offset := range []int{0, 8, 12, 28, 32, 34, 35, 36, len(original) - 1} {
		raw := bytes.Clone(original)
		raw[offset] ^= 0xff
		if _, err := store.decode(hash, raw); err == nil {
			t.Fatal("corruption accepted")
		}
	}
	for _, offset := range []int{34, 35, 36} {
		raw := bytes.Clone(original)
		raw[offset] = 255
		h := sha256.New()
		h.Write(store.domain[:])
		h.Write(hash[:])
		h.Write(raw[:len(raw)-sha256.Size])
		copy(raw[len(raw)-sha256.Size:], h.Sum(nil))
		if _, err := store.decode(hash, raw); err == nil {
			t.Fatal("valid-checksum invalid lifecycle accepted")
		}
	}
	if !store.Stats().Healthy || !bytes.Equal(retainedAdmission(t, store, hash).raw, f.raws[0]) {
		t.Fatal("refusal controls mutated retained bytes")
	}
}

func TestAdmissionStoreRetiredRetentionDoesNotFreeCapacity(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	r, store := storedRecoveryRelayer(t, f, filepath.Join(t.TempDir(), "admissions"), AdmissionStoreLimits{MaxRecords: 1})
	activateStoredRecovery(t, r)
	raw := f.raws[0]
	hash := signedAdmissionHash(t, raw)
	if _, err := r.Submit(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	e := r.byHash[hash]
	record, _ := admissionFromEntry(e, 0, admissionTTLExpired, admissionQueued)
	if err := store.apply([]admissionRecord{record}); err != nil {
		t.Fatal(err)
	}
	stats := store.Stats()
	if stats.RetainedRecords != 1 || stats.ActiveRecords != 0 || stats.RetainedRawBytes != int64(len(raw)) {
		t.Fatal("retirement silently freed retention")
	}
	other := send(t, f.key, 1, big.NewInt(1), 1)
	copy := record
	copy.hash = signedAdmissionHash(t, other)
	copy.raw = other
	copy.state = admissionActive
	copy.reason = admissionNoRetirement
	if err := store.apply([]admissionRecord{copy}); !errors.Is(err, ErrAdmissionStoreLimit) {
		t.Fatal("retired retention did not enforce capacity")
	}
}

func TestDurableConcurrentIdenticalAdmissionHasOneStoredIdentity(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	const callers = 16
	var entered atomic.Int32
	allEntered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
		if err != nil {
			t.Error("owned request unavailable")
			return
		}
		var request recoveryRequest
		if len(bytes.TrimSpace(raw)) > 0 && bytes.TrimSpace(raw)[0] == '{' && json.Unmarshal(raw, &request) != nil {
			t.Error("owned request refused")
			return
		}
		if request.Method == "eth_getBlockByNumber" && len(request.Params) > 0 && string(request.Params[0]) == `"latest"` {
			if entered.Add(1) == callers {
				close(allEntered)
			}
			select {
			case <-release:
			case <-req.Context().Done():
				return
			}
		}
		req.Body = io.NopCloser(bytes.NewReader(raw))
		f.serveEth(w, req)
	}))
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); server.Close() })
	r, store := storedRecoveryRelayer(t, f, filepath.Join(t.TempDir(), "admissions"), AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	eth, err := ethclient.Dial(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer eth.Close()
	r.cfg.Eth = eth
	var writes atomic.Int32
	original := store.write
	store.write = func(batch *leveldb.Batch, options *opt.WriteOptions) error {
		writes.Add(1)
		return original(batch, options)
	}
	errors := make(chan error, callers)
	var done sync.WaitGroup
	for i := 0; i < callers; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			hash, err := r.Submit(ctx, f.raws[0])
			if err == nil && hash != signedAdmissionHash(t, f.raws[0]) {
				err = ErrAdmissionStoreUnavailable
			}
			errors <- err
		}()
	}
	select {
	case <-allEntered:
	case <-time.After(4 * time.Second):
		t.Fatal("identical admissions did not reach the controlled shared boundary")
	}
	releaseOnce.Do(func() { close(release) })
	done.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal("identical admission lost idempotency", err)
		}
	}
	if writes.Load() != 1 || store.Stats().RetainedRecords != 1 || store.Stats().ActiveRecords != 1 || r.nQueued != 1 {
		t.Fatal("identical admission committed multiple identities")
	}
}

func TestDurableSettlementLifecyclePrecedesLiveMutation(t *testing.T) {
	for _, name := range []string{"included", "requeued", "nonce-used", "retries-exhausted"} {
		t.Run(name, func(t *testing.T) {
			f := recoveryFixtureFor(t, 130, nil)
			r, store := storedRecoveryRelayer(t, f, filepath.Join(t.TempDir(), "admissions"), AdmissionStoreLimits{})
			activateStoredRecovery(t, r)
			hash := signedAdmissionHash(t, f.raws[0])
			original := bytes.Clone(r.byHash[hash].raw)
			included := map[common.Hash]bool{}
			if name == "included" {
				included[hash] = true
			}
			if name == "nonce-used" {
				f.nonce = 1
			}
			if name == "retries-exhausted" {
				r.cfg.MaxRequeues = 0
			}
			r.settle(context.Background(), r.batches[0], included, true)
			record := retainedAdmission(t, store, hash)
			if !bytes.Equal(record.raw, original) || r.nPosted != 0 || len(r.batches) != 0 {
				t.Fatal("settlement changed bytes or retained the old batch")
			}
			if name == "requeued" {
				if record.state != admissionActive || record.phase != admissionQueued || record.requeues != 1 || r.nQueued != 1 || r.byHash[hash] == nil || !record.queuedAt.Equal(r.byHash[hash].queuedAt) {
					t.Fatal("requeue metadata diverged")
				}
			} else {
				want := map[string]byte{"included": admissionObservedIncluded, "nonce-used": admissionNonceUsed, "retries-exhausted": admissionRetriesExhausted}[name]
				if record.state != admissionRetired || record.reason != want || r.byHash[hash] != nil || r.nQueued != 0 {
					t.Fatal("retirement metadata diverged")
				}
			}
		})
	}
}

func TestDurableSettlementWriteFailureKeepsPostedBatch(t *testing.T) {
	f := recoveryFixtureFor(t, 130, nil)
	r, store := storedRecoveryRelayer(t, f, filepath.Join(t.TempDir(), "admissions"), AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	hash := signedAdmissionHash(t, f.raws[0])
	entry := r.byHash[hash]
	batch := r.batches[0]
	store.write = func(*leveldb.Batch, *opt.WriteOptions) error { return errors.New("private write failure") }
	r.settle(context.Background(), batch, map[common.Hash]bool{hash: true}, true)
	if r.Ready() || r.nPosted != 1 || r.nQueued != 0 || r.byHash[hash] != entry || len(r.batches) != 1 || r.batches[0] != batch {
		t.Fatal("failed retirement partially mutated live state")
	}
	if retainedAdmission(t, store, hash).state != admissionActive {
		t.Fatal("failed retirement changed stored state")
	}
}
