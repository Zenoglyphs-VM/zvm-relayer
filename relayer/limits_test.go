package relayer

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/Zenoglyphs-VM/zvm-core/derive"
)

func limitedRelayer(t *testing.T) *Relayer {
	t.Helper()
	var nonce atomic.Uint64
	return submitting(testRelayer(fakeChain(t, &nonce, zvm(1000))))
}

func TestAdmissionCapsAreTypedAndRefusedWorkRetainsNoResources(t *testing.T) {
	for _, resource := range []string{"transactions", "raw bytes", "senders", "sender transactions", "sender raw bytes", "nonce gap"} {
		t.Run(resource, func(t *testing.T) {
			r := limitedRelayer(t)
			key, _ := crypto.GenerateKey()
			other, _ := crypto.GenerateKey()
			raw := send(t, key, 1, big.NewInt(1), 10)
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
			if _, err := r.Submit(context.Background(), raw); err != nil {
				t.Fatal(err)
			}
			before := r.Snapshot()
			next := send(t, key, 2, big.NewInt(1), 10)
			if resource == "senders" {
				next = send(t, other, 0, big.NewInt(1), 10)
			}
			_, err := r.Submit(context.Background(), next)
			var capacity *CapacityError
			var rejected *RejectError
			if !errors.As(err, &capacity) || !errors.As(err, &rejected) || capacity.Resource != resource {
				t.Fatalf("resource refusal lost its type: %v", err)
			}
			after := r.Snapshot()
			if after.Queued != before.Queued || after.RetainedRawBytes != before.RetainedRawBytes || after.Senders != before.Senders {
				t.Fatal("refused admission retained queue resources")
			}
		})
	}
}

func TestAcceptedBytesAreOwnedAndRejectedReplacementPreservesTheOriginal(t *testing.T) {
	r := limitedRelayer(t)
	key, _ := crypto.GenerateKey()
	raw := send(t, key, 1, big.NewInt(1), 10)
	original := bytes.Clone(raw)
	r.cfg.Limits = Limits{MaxRawBytes: int64(len(raw))}
	hash, err := r.Submit(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0
	}
	if !bytes.Equal(r.byHash[hash].raw, original) {
		t.Fatal("caller buffer changed an acknowledged signed transaction")
	}
	if got, err := r.Submit(context.Background(), original); err != nil || got != hash {
		t.Fatalf("exact resubmission was not idempotent: %v", err)
	}
	// A correctly signed larger replacement must be refused before the
	// original hash or its byte reservation is removed.
	to := common.Address{19: 7}
	replacement, err := types.SignNewTx(key, r.signer, &types.DynamicFeeTx{
		ChainID: big.NewInt(testL2Chain), Nonce: 1, GasTipCap: big.NewInt(20), GasFeeCap: big.NewInt(21),
		Gas: 30_000, To: &to, Value: big.NewInt(1), Data: bytes.Repeat([]byte{1}, 128),
	})
	if err != nil {
		t.Fatal(err)
	}
	replacementRaw, err := replacement.MarshalBinary()
	if err != nil || len(replacementRaw) <= len(original) {
		t.Fatalf("larger replacement fixture is invalid: %v", err)
	}
	if _, err := r.Submit(context.Background(), replacementRaw); err == nil {
		t.Fatal("over-budget replacement was admitted")
	}
	if r.byHash[hash] == nil || !bytes.Equal(r.byHash[hash].raw, original) || r.retainedBytes != int64(len(original)) {
		t.Fatal("refused replacement destroyed its original")
	}
}

func TestBurstAdmissionAndExpiryKeepCountBytesAndSenderStateBounded(t *testing.T) {
	r := limitedRelayer(t)
	r.cfg.Limits = Limits{MaxTransactions: 3}
	key, _ := crypto.GenerateKey()
	raws := make([][]byte, 16)
	for i := range raws {
		raws[i] = send(t, key, uint64(i+1), big.NewInt(1), 10)
	}
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for _, raw := range raws {
		wg.Add(1)
		go func(raw []byte) {
			defer wg.Done()
			if _, err := r.Submit(context.Background(), raw); err == nil {
				accepted.Add(1)
			} else {
				var capacity *CapacityError
				if !errors.As(err, &capacity) {
					t.Errorf("unexpected burst refusal: %v", err)
				}
			}
		}(raw)
	}
	wg.Wait()
	if accepted.Load() != 3 || len(r.byHash) != 3 || r.nQueued != 3 {
		t.Fatalf("burst overspent count budget: accepted=%d live=%d", accepted.Load(), len(r.byHash))
	}
	var expected int64
	for _, e := range r.byHash {
		expected += int64(len(e.raw))
		e.queuedAt = time.Now().Add(-time.Hour)
	}
	if expected != r.retainedBytes {
		t.Fatal("retained byte accounting disagrees with accepted payloads")
	}
	for _, s := range r.senders {
		s.lastUse = time.Now().Add(-time.Hour)
	}
	r.sweep()
	if len(r.byHash) != 0 || len(r.senders) != 0 || r.retainedBytes != 0 || r.nQueued != 0 {
		t.Fatal("expired gapped work did not return logical resources to baseline")
	}
}

func TestHeadAccountCacheStaysBoundedAcrossDistinctSenders(t *testing.T) {
	r := limitedRelayer(t)
	r.cfg.Limits = Limits{MaxSenders: 2}
	for i := byte(1); i < 10; i++ {
		if _, _, err := r.headFor(context.Background(), common.Address{19: i}); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.head.accounts) > 2 {
		t.Fatal("head account cache exceeded its sender budget")
	}
}

func TestAdmissionConcurrencyAndDeadlineDoNotLeaveReservations(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		once.Do(func() { close(started) })
		select {
		case <-req.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	eth, err := ethclient.Dial(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer eth.Close()
	r := submitting(testRelayer(eth))
	r.cfg.Limits = Limits{MaxConcurrent: 1}
	key, _ := crypto.GenerateKey()
	raw := send(t, key, 0, big.NewInt(1), 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := r.Submit(ctx, raw); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first admission did not reach the controlled RPC")
	}
	if _, err := r.Submit(context.Background(), raw); err == nil {
		t.Fatal("excess admission bypassed concurrency budget")
	} else {
		var capacity *CapacityError
		if !errors.As(err, &capacity) || capacity.Resource != "concurrent admission" {
			t.Fatalf("wrong refusal: %v", err)
		}
	}
	if _, err := r.NextNonce(context.Background(), common.Address{19: 1}); err == nil {
		t.Fatal("pending nonce bypassed the shared admission budget")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled admission did not release")
	}
	r.adopted = make(chan struct{})
	r.cfg.Limits.AdmissionTimeout = 20 * time.Millisecond
	if _, err := r.Submit(context.Background(), raw); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unready recovery did not honor admission deadline: %v", err)
	}
	if len(r.admission) != 0 || len(r.byHash) != 0 || r.retainedBytes != 0 {
		t.Fatal("failed admission retained work or slots")
	}
}

func TestPostedWorkKeepsItsCountAndByteReservations(t *testing.T) {
	r := limitedRelayer(t)
	r.cfg.Limits = Limits{MaxTransactions: 1}
	key, _ := crypto.GenerateKey()
	raw := send(t, key, 0, big.NewInt(1), 10)
	hash, err := r.Submit(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	s := r.senders[crypto.PubkeyToAddress(key.PublicKey)]
	post(r, s, 0)
	if r.retainedBytes != int64(len(raw)) || r.byHash[hash] == nil || len(s.queued) != 0 || len(s.posted) != 1 {
		t.Fatal("moving to posted released its retained reservation")
	}
	if _, err := r.Submit(context.Background(), send(t, key, 1, big.NewInt(1), 10)); err == nil {
		t.Fatal("posted work no longer counted against admission")
	}
}

func TestRecoveryLimitsAndMalformedHistoryLeaveAdmissionUnavailable(t *testing.T) {
	key, _ := crypto.GenerateKey()
	other, _ := crypto.GenerateKey()
	raws := [][]byte{send(t, key, 0, big.NewInt(1), 10), send(t, key, 1, big.NewInt(1), 10)}
	for _, name := range []string{"transactions", "raw bytes", "senders", "sender transactions", "sender raw bytes", "malformed batch"} {
		t.Run(name, func(t *testing.T) {
			inputs := raws
			limits := DefaultLimits()
			switch name {
			case "transactions":
				limits.MaxTransactions = 1
			case "raw bytes":
				limits.MaxRawBytes = int64(len(raws[0]))
			case "senders":
				limits.MaxSenders = 1
				inputs = [][]byte{raws[0], send(t, other, 0, big.NewInt(1), 10)}
			case "sender transactions":
				limits.MaxSenderTransactions = 1
			case "sender raw bytes":
				limits.MaxSenderRawBytes = int64(len(raws[0]))
			}
			f := recoveryFixtureFor(t, 130, inputs)
			if name == "malformed batch" {
				f.blocks[1].Data = []byte{derive.MsgBatch, 255}
				f.rebuild()
			}
			r := f.relayer()
			r.cfg.Limits = limits
			r.adopted = make(chan struct{})
			if r.Ready() {
				t.Fatal("recovery advertised readiness before it ran")
			}
			if err := r.Run(context.Background()); err == nil {
				t.Fatal("partial recovery started workers")
			} else if name != "malformed batch" {
				var capacity *CapacityError
				if !errors.As(err, &capacity) {
					t.Fatalf("recovery lost typed capacity refusal: %v", err)
				}
			}
			status := r.Snapshot()
			if status.Ready || status.Queued != 0 || status.PostedPending != 0 || status.RetainedRawBytes != 0 || status.Senders != 0 {
				t.Fatal("recovery budget refusal installed partial state")
			}
			if _, err := r.Submit(context.Background(), raws[0]); err == nil {
				t.Fatal("failed recovery admitted new work")
			}
		})
	}
}

func TestFailedStartupRecoveryCannotEnableAdmissionOrPendingNonce(t *testing.T) {
	f := recoveryFixtureFor(t, 130, nil)
	f.mode = "remote_error"
	r := f.relayer()
	r.adopted = make(chan struct{})
	if err := r.Run(context.Background()); err == nil || r.Ready() {
		t.Fatal("failed recovery enabled admission")
	}
	if _, err := r.Submit(context.Background(), f.raws[0]); err == nil {
		t.Fatal("failed recovery admitted signed bytes")
	}
	if _, err := r.NextNonce(context.Background(), common.Address{19: 1}); err == nil {
		t.Fatal("failed recovery returned pending nonce")
	}
}

func TestVolatileAdmissionCanRebroadcastTheSameSignedTransactionAfterRestart(t *testing.T) {
	r := limitedRelayer(t)
	key, _ := crypto.GenerateKey()
	raw := send(t, key, 0, big.NewInt(1), 10)
	hash, err := r.Submit(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if r.Snapshot().AdmissionMode != "volatile" {
		t.Fatal("admission claimed persistence")
	}
	restarted := submitting(testRelayer(r.cfg.Eth))
	got, err := restarted.Submit(context.Background(), raw)
	if err != nil || got != hash || !bytes.Equal(restarted.byHash[hash].raw, raw) {
		t.Fatalf("same signed bytes changed identity on re-admission: %v", err)
	}
}

func TestAdmissionByteComparisonCannotWrapPastItsBudget(t *testing.T) {
	r := limitedRelayer(t)
	limits := DefaultLimits()
	limits.MaxRawBytes = int64(^uint64(0) >> 1)
	r.retainedBytes = limits.MaxRawBytes - 1
	var refused *CapacityError
	if err := r.checkCapacity(limits, common.Address{1}, 8, nil); !errors.As(err, &refused) || refused.Resource != "raw bytes" {
		t.Fatal("global signed-byte comparison wrapped")
	}
	r.retainedBytes = 0
	s := &sender{rawBytes: limits.MaxRawBytes - 1}
	r.senders[common.Address{1}] = s
	limits.MaxSenderRawBytes = limits.MaxRawBytes
	if err := r.checkCapacity(limits, common.Address{1}, 8, nil); !errors.As(err, &refused) || refused.Resource != "sender raw bytes" {
		t.Fatal("sender signed-byte comparison wrapped")
	}
}
