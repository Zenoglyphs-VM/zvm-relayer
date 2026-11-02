package relayer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math/big"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Zenoglyphs-VM/zvm-core/derive"
	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func boundedJournalImage(t *testing.T, store *AdmissionStore) map[string][32]byte {
	t.Helper()
	image := map[string][32]byte{}
	iter := store.db.NewIterator(nil, &opt.ReadOptions{Strict: opt.StrictAll})
	defer iter.Release()
	for iter.Next() {
		image[string(iter.Key())] = sha256.Sum256(iter.Value())
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	return image
}

func assertBoundedAdmissionAccounting(t *testing.T, r *Relayer) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var total int64
	bySender := map[common.Address]int64{}
	for _, e := range r.byHash {
		total += int64(len(e.raw))
		bySender[e.from] += int64(len(e.raw))
	}
	if total != r.retainedBytes {
		t.Fatalf("retained signed bytes %d, want %d", r.retainedBytes, total)
	}
	for address, sender := range r.senders {
		if sender.rawBytes != bySender[address] {
			t.Fatal("sender byte reservation disagrees with retained original bytes")
		}
	}
	if len(r.byHash) != r.nQueued+r.nPosted {
		t.Fatal("queued and posted reservations disagree with retained identities")
	}
}

func TestDurableResourceRefusalPrecedesJournalWrite(t *testing.T) {
	for _, resource := range []string{"transactions", "raw bytes", "senders", "sender transactions", "sender raw bytes", "nonce gap"} {
		t.Run(resource, func(t *testing.T) {
			f := recoveryFixtureFor(t, 1, nil)
			r, store := storedRecoveryRelayer(t, f, filepath.Join(t.TempDir(), "admissions"), AdmissionStoreLimits{})
			raw := send(t, f.key, 1, big.NewInt(1), 10)
			limits := DefaultLimits()
			switch resource {
			case "transactions":
				limits.MaxTransactions = 1
			case "raw bytes":
				limits.MaxRawBytes = int64(len(raw))
			case "senders":
				limits.MaxSenders = 1
			case "sender transactions":
				limits.MaxSenderTransactions = 1
			case "sender raw bytes":
				limits.MaxSenderRawBytes = int64(len(raw))
			case "nonce gap":
				limits.MaxNonceGap = 1
			}
			r.cfg.Limits = limits
			activateStoredRecovery(t, r)
			hash, err := r.Submit(context.Background(), raw)
			if err != nil {
				t.Fatal(err)
			}
			before, stats := boundedJournalImage(t, store), store.Stats()
			beforeStatus := r.Snapshot()
			var writes int
			originalWrite := store.write
			store.write = func(batch *leveldb.Batch, options *opt.WriteOptions) error {
				writes++
				return originalWrite(batch, options)
			}
			next := send(t, f.key, 2, big.NewInt(1), 10)
			if resource == "senders" {
				other, err := crypto.GenerateKey()
				if err != nil {
					t.Fatal(err)
				}
				next = send(t, other, 0, big.NewInt(1), 10)
			}
			_, err = r.Submit(context.Background(), next)
			var capacity *CapacityError
			if !errors.As(err, &capacity) || capacity.Resource != resource {
				t.Fatalf("wrong typed refusal: %v", err)
			}
			if writes != 0 || !reflect.DeepEqual(before, boundedJournalImage(t, store)) || store.Stats() != stats {
				t.Fatal("resource refusal changed durable records or store health")
			}
			after := r.Snapshot()
			if after.Queued != beforeStatus.Queued || after.Senders != beforeStatus.Senders || after.RetainedRawBytes != beforeStatus.RetainedRawBytes {
				t.Fatal("resource refusal changed accepted live reservations")
			}
			if got, err := r.Submit(context.Background(), raw); err != nil || got != hash || writes != 0 {
				t.Fatal("a full budget lost the original durable idempotent acknowledgment", err)
			}
			assertBoundedAdmissionAccounting(t, r)
		})
	}
}

func TestDurableResourceRecoveryCountsBothSourcesOnce(t *testing.T) {
	for _, resource := range []string{"fits", "transactions", "raw bytes", "sender transactions", "sender raw bytes"} {
		t.Run(resource, func(t *testing.T) {
			f := recoveryFixtureFor(t, 1, nil)
			path := filepath.Join(t.TempDir(), "admissions")
			r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
			activateStoredRecovery(t, r)
			first, second := f.raws[0], send(t, f.key, 1, big.NewInt(1), 10)
			for _, raw := range [][]byte{first, second} {
				if _, err := r.Submit(context.Background(), raw); err != nil {
					t.Fatal(err)
				}
			}
			original := boundedJournalImage(t, store)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			payload, err := derive.EncodeBatch([][]byte{first}, derive.BatchZlib)
			if err != nil {
				t.Fatal(err)
			}
			f.blocks = append(f.blocks, &znn.AccountBlock{Version: 1, ChainIdentifier: 69,
				BlockType: znn.BlockTypeUserSend, Address: f.address, ToAddress: rollup.Inbox, Height: 2, Data: payload})
			f.rebuild()
			next, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
			limits := DefaultLimits()
			expectedBytes := int64(len(first) + len(second))
			limits.MaxTransactions, limits.MaxRawBytes, limits.MaxSenderTransactions, limits.MaxSenderRawBytes = 2, expectedBytes, 2, expectedBytes
			switch resource {
			case "transactions":
				limits.MaxTransactions = 1
			case "raw bytes":
				limits.MaxRawBytes = int64(len(first))
			case "sender transactions":
				limits.MaxSenderTransactions = 1
			case "sender raw bytes":
				limits.MaxSenderRawBytes = int64(len(first))
			}
			next.cfg.Limits = limits
			if err := nextStore.attach(next.cfg); err != nil {
				t.Fatal(err)
			}
			err = next.recoverInFlight(context.Background())
			if resource != "fits" {
				var capacity *CapacityError
				if !errors.As(err, &capacity) || capacity.Resource != resource {
					t.Fatalf("wrong recovery refusal: %v", err)
				}
				if !reflect.DeepEqual(original, boundedJournalImage(t, nextStore)) || !nextStore.Stats().Healthy || len(next.byHash) != 0 || next.retainedBytes != 0 {
					t.Fatal("failed mixed-source reconstruction changed the original journal or live reservations")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if next.nQueued != 1 || next.nPosted != 1 || len(next.byHash) != 2 || next.retainedBytes != expectedBytes || nextStore.Stats().ActiveRecords != 2 {
				t.Fatal("chain/journal duplicate was double-charged or journal-only admission escaped the shared budget")
			}
			for _, raw := range [][]byte{first, second} {
				hash := signedAdmissionHash(t, raw)
				if !bytes.Equal(next.byHash[hash].raw, raw) || !bytes.Equal(retainedAdmission(t, nextStore, hash).raw, raw) {
					t.Fatal("recovery changed original signed identity")
				}
			}
			if f.writes.Load() != 0 {
				t.Fatal("resource reconciliation published to NoM")
			}
			assertBoundedAdmissionAccounting(t, next)
		})
	}
}

func TestDurableResourceConcurrentAdmissionPersistsOnlyReservedWork(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	r, store := storedRecoveryRelayer(t, f, filepath.Join(t.TempDir(), "admissions"), AdmissionStoreLimits{})
	r.cfg.Limits = Limits{MaxTransactions: 3}
	activateStoredRecovery(t, r)
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for nonce := uint64(1); nonce <= 16; nonce++ {
		raw := send(t, f.key, nonce, big.NewInt(1), 10)
		wg.Add(1)
		go func(raw []byte) {
			defer wg.Done()
			if _, err := r.Submit(context.Background(), raw); err == nil {
				accepted.Add(1)
			} else {
				var capacity *CapacityError
				if !errors.As(err, &capacity) {
					t.Errorf("unexpected concurrent refusal: %v", err)
				}
			}
		}(raw)
	}
	wg.Wait()
	if accepted.Load() != 3 || r.nQueued != 3 || store.Stats().ActiveRecords != 3 || store.Stats().RetainedRecords != 3 {
		t.Fatal("concurrent work overspent or durably retained refused admissions")
	}
	assertBoundedAdmissionAccounting(t, r)
}

func TestDurableResourceUnknownWriteInstallsNothingUntilReopen(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	r.cfg.Limits = Limits{MaxTransactions: 1}
	activateStoredRecovery(t, r)
	originalWrite := store.write
	store.write = func(batch *leveldb.Batch, options *opt.WriteOptions) error {
		if err := originalWrite(batch, options); err != nil {
			return err
		}
		return errors.New("controlled unknown result after committed synchronous write")
	}
	raw := f.raws[0]
	if _, err := r.Submit(context.Background(), raw); !errors.Is(err, ErrAdmissionStoreUnknown) {
		t.Fatal("ambiguous write did not remain UNKNOWN", err)
	}
	if r.Ready() || len(r.byHash) != 0 || r.retainedBytes != 0 || r.nQueued != 0 {
		t.Fatal("ambiguous write installed or acknowledged a live reservation")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	next, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	next.cfg.Limits = r.cfg.Limits
	activateStoredRecovery(t, next)
	if next.nQueued != 1 || next.retainedBytes != int64(len(raw)) || nextStore.Stats().ActiveRecords != 1 {
		t.Fatal("verified reopen did not install exactly the committed reservation")
	}
	if got, err := next.Submit(context.Background(), raw); err != nil || got != signedAdmissionHash(t, raw) {
		t.Fatal("reopen lost the exact original acknowledgment", err)
	}
	assertBoundedAdmissionAccounting(t, next)
}

func TestDurableResourceReplacementSeparatesActiveAndRetiredBudgets(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	original := send(t, f.key, 1, big.NewInt(1), 10)
	r.cfg.Limits = Limits{MaxTransactions: 1, MaxRawBytes: int64(len(original))}
	activateStoredRecovery(t, r)
	hash, err := r.Submit(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	to := common.Address{19: 7}
	replacement, err := types.SignNewTx(f.key, r.signer, &types.DynamicFeeTx{
		ChainID: r.cfg.Rollup.L2ChainIDBig(), Nonce: 1, Gas: 30000,
		GasTipCap: big.NewInt(20), GasFeeCap: big.NewInt(21), To: &to, Value: big.NewInt(1), Data: bytes.Repeat([]byte{1}, 128)})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := replacement.MarshalBinary()
	if err != nil || len(raw) <= len(original) {
		t.Fatal("invalid larger replacement", err)
	}
	before := boundedJournalImage(t, store)
	if _, err := r.Submit(context.Background(), raw); err == nil {
		t.Fatal("over-budget durable replacement admitted")
	}
	if !reflect.DeepEqual(before, boundedJournalImage(t, store)) || r.byHash[hash] == nil || retainedAdmission(t, store, hash).state != admissionActive {
		t.Fatal("replacement refusal retired or lost the acknowledged original")
	}
	r.cfg.Limits.MaxRawBytes = int64(len(raw))
	if got, err := r.Submit(context.Background(), raw); err != nil || got != replacement.Hash() {
		t.Fatal("fitting replacement refused", err)
	}
	if r.byHash[hash] != nil || r.retainedBytes != int64(len(raw)) || r.nQueued != 1 {
		t.Fatal("replacement leaked active count or bytes")
	}
	retired := retainedAdmission(t, store, hash)
	if retired.state != admissionRetired || retired.reason != admissionReplaced || !bytes.Equal(retired.raw, original) {
		t.Fatal("retired original audit bytes changed")
	}
	stats := store.Stats()
	if stats.RetainedRecords != 2 || stats.ActiveRecords != 1 || stats.RetainedRawBytes != int64(len(raw)+len(original)) {
		t.Fatal("retired audit bytes were omitted from independent retention")
	}
	assertBoundedAdmissionAccounting(t, r)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	next, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	next.cfg.Limits = r.cfg.Limits
	activateStoredRecovery(t, next)
	if next.retainedBytes != int64(len(raw)) || next.nQueued != 1 || nextStore.Stats().RetainedRecords != 2 {
		t.Fatal("replacement reopen confused active and retained budgets")
	}
	assertBoundedAdmissionAccounting(t, next)
}

func TestDurableResourceLateRecoveryRefusalDoesNotCommitRetirement(t *testing.T) {
	f := recoveryFixtureFor(t, 1, nil)
	path := filepath.Join(t.TempDir(), "admissions")
	r, store := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	activateStoredRecovery(t, r)
	raws := [][]byte{f.raws[0], send(t, f.key, 1, big.NewInt(1), 10), send(t, f.key, 2, big.NewInt(1), 10)}
	for _, raw := range raws {
		if _, err := r.Submit(context.Background(), raw); err != nil {
			t.Fatal(err)
		}
	}
	before := boundedJournalImage(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	f.nonce = 1
	payload, err := derive.EncodeBatch([][]byte{raws[1]}, derive.BatchZlib)
	if err != nil {
		t.Fatal(err)
	}
	f.blocks = append(f.blocks, &znn.AccountBlock{Version: 1, ChainIdentifier: 69, BlockType: znn.BlockTypeUserSend, Address: f.address, ToAddress: rollup.Inbox, Height: 2, Data: payload})
	f.rebuild()
	next, nextStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	next.cfg.Limits = Limits{MaxTransactions: 1}
	if err := nextStore.attach(next.cfg); err != nil {
		t.Fatal(err)
	}
	err = next.recoverInFlight(context.Background())
	var capacity *CapacityError
	if !errors.As(err, &capacity) || capacity.Resource != "transactions" {
		t.Fatal("mixed recovery did not refuse its shared count", err)
	}
	if !reflect.DeepEqual(before, boundedJournalImage(t, nextStore)) || nextStore.Stats().ActiveRecords != 3 || !nextStore.Stats().Healthy || len(next.byHash) != 0 {
		t.Fatal("late recovery refusal committed used-nonce retirement or partial installation")
	}
	if err := nextStore.Close(); err != nil {
		t.Fatal(err)
	}
	final, finalStore := storedRecoveryRelayer(t, f, path, AdmissionStoreLimits{})
	final.cfg.Limits = Limits{MaxTransactions: 2}
	activateStoredRecovery(t, final)
	old := retainedAdmission(t, finalStore, signedAdmissionHash(t, raws[0]))
	if old.state != admissionRetired || old.reason != admissionNonceUsed || !bytes.Equal(old.raw, raws[0]) {
		t.Fatal("fitting recovery lost the original used-nonce audit record")
	}
	if final.nQueued != 1 || final.nPosted != 1 || finalStore.Stats().ActiveRecords != 2 || finalStore.Stats().RetainedRecords != 3 {
		t.Fatal("fitting recovery did not atomically install the original two active identities")
	}
	assertBoundedAdmissionAccounting(t, final)
}
