package relayer

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"math/big"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

func resourceWorkloadRaw(t *testing.T, key *ecdsa.PrivateKey, nonce uint64) []byte {
	t.Helper()
	to := common.Address{7}
	tx, err := types.SignNewTx(key, types.LatestSignerForChainID(big.NewInt(testL2Chain)), &types.DynamicFeeTx{ChainID: big.NewInt(testL2Chain), Nonce: nonce, Gas: 50000, GasTipCap: big.NewInt(10), GasFeeCap: big.NewInt(11), To: &to, Value: big.NewInt(1), Data: make([]byte, 1024)})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestAdmissionBurstHeapReturnsToItsWarmedBound(t *testing.T) {
	const cycles = 128
	r := limitedRelayer(t)
	r.cfg.Limits = Limits{MaxTransactions: 64, MaxRawBytes: 128 << 10, MaxSenders: 8, MaxSenderTransactions: 8, MaxSenderRawBytes: 16 << 10}
	keys := make([]*ecdsa.PrivateKey, 8)
	for i := range keys {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = key
	}
	// Fixture input bytes remain live throughout the comparison. They are not
	// silently dropped to manufacture a lower post-workload heap baseline.
	inputs := make([][][]byte, cycles+2)
	for cycle := range inputs {
		for _, key := range keys {
			for n := uint64(1); n <= 8; n++ {
				inputs[cycle] = append(inputs[cycle], resourceWorkloadRaw(t, key, n+uint64(cycle)*8))
			}
		}
	}
	expire := func() {
		r.mu.Lock()
		for _, entry := range r.byHash {
			entry.queuedAt = time.Now().Add(-time.Hour)
		}
		for _, s := range r.senders {
			s.lastUse = time.Now().Add(-time.Hour)
		}
		r.mu.Unlock()
		r.sweep()
		status := r.Snapshot()
		if status.Queued != 0 || status.PostedPending != 0 || status.RetainedRawBytes != 0 || status.Senders != 0 || len(r.admission) != 0 {
			t.Fatal("expired gapped workload retained logical resources")
		}
	}
	run := func(raws [][]byte) {
		for _, raw := range raws {
			if _, err := r.Submit(context.Background(), raw); err != nil {
				t.Fatalf("declared bounded workload was rejected: %v", err)
			}
		}
		status := r.Snapshot()
		if status.Queued != 64 || status.RetainedRawBytes > 128<<10 || status.Senders != 8 {
			t.Fatal("bounded workload exceeded count/byte/sender profile")
		}
		// A new sender cannot consume another slot at the warmed high water.
		extra, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		_, err = r.Submit(context.Background(), resourceWorkloadRaw(t, extra, 1))
		var capacity *CapacityError
		if !errors.As(err, &capacity) {
			t.Fatal("full workload failed to produce typed backpressure")
		}
		expire()
	}
	run(inputs[0])
	run(inputs[1])
	runtime.GC()
	debug.FreeOSMemory()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for cycle := 2; cycle < len(inputs); cycle++ {
		run(inputs[cycle])
		runtime.GC()
		debug.FreeOSMemory()
		runtime.ReadMemStats(&after)
		if after.HeapAlloc > before.HeapAlloc+8<<20 {
			t.Fatal("post-GC heap grew past the warmed bounded baseline")
		}
	}
	runtime.KeepAlive(inputs)
	runtime.KeepAlive(keys)
	record := map[string]any{"case": "GC_assisted_admission_heap_liveness", "cycles": cycles, "transactions_per_cycle": 64, "baseline_heap_bytes": before.HeapAlloc, "final_heap_bytes": after.HeapAlloc,
		"allowed_heap_delta_bytes": 8 << 20, "final_queued": 0, "final_posted": 0, "final_retained_bytes": 0, "final_senders": 0, "final_admission_slots": 0,
		"GC_assisted": true, "production_OS_RSS_reclamation_proof": false}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
}
