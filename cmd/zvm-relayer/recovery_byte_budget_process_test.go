package main

import (
	"bytes"
	"encoding/json"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
	"strconv"
	"testing"
)

// Measure serialized fixture envelopes independently of the recovery reader.
// Every page includes its original Data, including already executed material.
func recoveryProcessByteBudgets(t *testing.T, f *recoveryProcessFixture) (int64, int64) {
	t.Helper()
	results := []any{znn.PlasmaInfo{CurrentPlasma: 105_000_000, MaxPlasma: 105_000_000}, f.blocks[len(f.blocks)-1]}
	for from := 0; from < len(f.blocks); from += 64 {
		end := min(from+64, len(f.blocks))
		results = append(results, znn.BlockList{Count: uint64(len(f.blocks)), List: f.blocks[from:end]})
	}
	results = append(results, f.blocks[len(f.blocks)-1])
	var largest, total int64
	for i, result := range results {
		raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": i + 1, "result": result})
		if err != nil {
			t.Fatal(err)
		}
		size := int64(len(raw) + 1) // The actual JSON encoder adds one newline.
		largest = max(largest, size)
		total += size
	}
	return max(int64(1024), largest+64), total + 64
}

func recoveryCLIByteBudget(t *testing.T, binary string) {
	t.Helper()
	f := newRecoveryProcessFixture(t, "pending")
	original := append([]byte(nil), f.raw...)
	single, total := recoveryProcessByteBudgets(t, f)
	if total <= single {
		t.Fatal("fixture cannot distinguish per-response and aggregate budgets")
	}
	failed := f.startWithRecoveryFlags(binary, "65536", "-recovery-max-response-bytes", strconv.FormatInt(single, 10), "-recovery-max-total-response-bytes", strconv.FormatInt(single, 10))
	if failed.wait(t) == nil || f.writes.Load() != 0 || f.pages.Load() == 0 {
		t.Fatal("small aggregate budget did not refuse partial history without publishing")
	}
	failed.output.mu.Lock()
	refused := bytes.Contains(failed.output.buffer.Bytes(), []byte("recovery total response byte limit exceeded"))
	failed.output.mu.Unlock()
	if !refused {
		t.Fatal("actual CLI did not identify the aggregate byte-budget refusal")
	}
	before := f.pages.Load()
	recovered := f.startWithRecoveryFlags(binary, "65536", "-recovery-max-response-bytes", strconv.FormatInt(single, 10), "-recovery-max-total-response-bytes", strconv.FormatInt(total, 10))
	recoveryHTTPStatus(t, recovered.url+"/health", 200)
	status := recoveryProcessSnapshot(t, recovered)
	if !status.Ready || status.RecoveryStatus != "complete" || status.AdmissionDurable || status.AdmissionMode != "volatile" || status.Queued != 0 || status.PostedPending != 1 || recoveryProcessSubmit(t, recovered, original) != f.hash {
		t.Fatal("explicit larger aggregate budget failed complete original-hash recovery")
	}
	recovered.kill(t)
	if f.pages.Load()-before != 3 || f.writes.Load() != 0 || !bytes.Equal(f.raw, original) {
		t.Fatal("budget selection changed source bytes, scanned a suffix or republished")
	}
	t.Logf(`{"case":"operator_response_byte_budget","account_blocks":130,"single_response_limit":%d,"refused_total_limit":%d,"accepted_total_limit":%d,"complete_recovery_pages":3,"processes_joined":2,"write_RPCs":0,"original_hash_preserved":true,"volatile_admission":true}`, single, single, total)
}
