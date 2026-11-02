package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
	"github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func canonicalRecoveryStoredWire(t *testing.T, directory string, hash common.Hash, expected []byte) {
	t.Helper()
	db, err := leveldb.OpenFile(filepath.Join(directory, "data"), &opt.Options{ReadOnly: true, Strict: opt.StrictAll, ErrorIfMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	value, err := db.Get(append([]byte("t/"), hash[:]...), &opt.ReadOptions{Strict: opt.StrictAll})
	closeErr := db.Close()
	if err != nil || closeErr != nil || len(value) < 37 || value[36] != 0 && value[36] != 2 {
		t.Fatal("unused nonce was not retained as queued or posted after join")
	}
	durableCLIStoredWire(t, directory, hash, expected, value[36])
}

func TestCanonicalRecoveryCLIProcesses(t *testing.T) {
	binary := os.Getenv("ZVM_RELAYER_RECOVERY_BINARY")
	if binary == "" {
		t.Skip("requires the declared actual race-instrumented production CLI job")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("production CLI binary must be explicit")
	}
	for _, mode := range []string{"volatile", "local-sync"} {
		t.Run(mode, func(t *testing.T) {
			f := newRecoveryProcessFixture(t, "pending")
			original := bytes.Clone(f.raw)
			block := f.blocks[1]
			before := block.Hash
			block.ConfirmationDetail = &znn.ConfirmationDetail{MomentumHeight: 1, NumConfirmations: 1}
			cfg := &rollup.Config{L1StartHeight: 1}
			if block.ComputeHash() != before || f.header.Number.Uint64() < cfg.L2BlockForMomentum(1) {
				t.Fatal("fixture did not preserve hash at the old metadata skip boundary")
			}
			var queries atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				raw, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
				if err != nil {
					t.Error("owned execution request unavailable")
					return
				}
				var in recoveryProcessRequest
				if len(bytes.TrimSpace(raw)) > 0 && bytes.TrimSpace(raw)[0] == '{' {
					if json.Unmarshal(raw, &in) != nil {
						t.Error("owned execution request refused")
						return
					}
				}
				if in.Method == "eth_getTransactionCount" && len(in.Params) == 2 && len(bytes.TrimSpace(in.Params[1])) > 0 && bytes.TrimSpace(in.Params[1])[0] == '{' {
					var selector struct {
						BlockHash        common.Hash
						RequireCanonical bool
					}
					if json.Unmarshal(in.Params[1], &selector) != nil || selector.BlockHash != f.header.Hash() || !selector.RequireCanonical {
						t.Error("actual CLI nonce was not selected by the frozen canonical hash")
						return
					}
					queries.Add(1)
				}
				req.Body = io.NopCloser(bytes.NewReader(raw))
				f.execution(w, req)
			}))
			defer server.Close()
			directory := filepath.Join(t.TempDir(), "admissions")
			for attempt := 0; attempt < 2; attempt++ {
				args := []string{"-eth", server.URL}
				if mode == "local-sync" {
					args = append(args, "-admission-store", directory)
				}
				p := f.startWithRecoveryFlags(binary, "65536", args...)
				recoveryHTTPStatus(t, p.url+"/health", 200)
				status := recoveryProcessSnapshot(t, p)
				if !status.Ready || status.AdmissionMode != mode || status.AdmissionDurable != (mode == "local-sync") || status.Queued+status.PostedPending != 1 {
					t.Fatal("derived-looking metadata discarded the still-unused canonical nonce")
				}
				if recoveryProcessSubmit(t, p, original) != f.hash {
					t.Fatal("canonical recovery changed the original signed identity")
				}
				p.kill(t)
				if mode == "local-sync" {
					canonicalRecoveryStoredWire(t, directory, f.hash, original)
				}
			}
			if queries.Load() != 2 || f.pages.Load() != 6 || f.writes.Load() != 0 || !bytes.Equal(f.raw, original) {
				t.Fatal("canonical recovery skipped history, repeated a nonce query or wrote")
			}
			record := map[string]any{"case": mode, "process_lifetimes": 2, "processes_joined": 2, "forced_stops": 2,
				"account_blocks": 130, "complete_recovery_pages": 6, "canonical_hash_selected_nonce_queries": 2,
				"confirmation_metadata_keeps_account_hash": true, "unused_nonce_retained": true, "original_hash_acknowledged_twice": true,
				"exact_signed_bytes_retained": true, "local_store_inspected_after_join": mode == "local-sync", "write_RPCs": 0}
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			t.Log("CANONICAL-RECOVERY-CLI " + string(raw))
		})
	}
}
