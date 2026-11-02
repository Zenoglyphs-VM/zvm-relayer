package relayer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Zenoglyphs-VM/zvm-core/derive"
	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

func canonicalRecoveryHash(t *testing.T, raw []byte) common.Hash {
	t.Helper()
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(raw); err != nil {
		t.Fatal(err)
	}
	return tx.Hash()
}

func TestRecoveryCanonicalNonceOwnsRetentionDecision(t *testing.T) {
	for _, name := range []string{"unconfirmed-unused", "derived-unused", "unconfirmed-used", "derived-used"} {
		t.Run(name, func(t *testing.T) {
			f := recoveryFixtureFor(t, 130, nil)
			r := f.relayer()
			original := bytes.Clone(f.raws[0])
			if strings.HasPrefix(name, "derived-") {
				before := f.blocks[1].Hash
				f.blocks[1].ConfirmationDetail = &znn.ConfirmationDetail{MomentumHeight: 1}
				if f.blocks[1].ComputeHash() != before || f.header.Number.Uint64() < r.cfg.Rollup.L2BlockForMomentum(1) {
					t.Fatal("fixture did not exercise unchanged hash and previously skipped metadata")
				}
			}
			used := strings.HasSuffix(name, "-used")
			if used {
				f.nonce = 1
				r.cfg.Recovery = RecoveryLimits{MaxRawBytes: 1, MaxTransactions: 1}
			}
			if err := r.recoverInFlight(context.Background()); err != nil {
				t.Fatal(err)
			}
			hash := canonicalRecoveryHash(t, original)
			if used {
				if r.nPosted != 0 || len(r.byHash) != 0 || len(r.batches) != 0 {
					t.Fatal("used canonical nonce was retained")
				}
			} else if r.nPosted != 1 || r.nQueued != 0 || len(r.batches) != 1 || r.byHash[hash] == nil || !bytes.Equal(r.byHash[hash].raw, original) {
				t.Fatal("unused canonical nonce was discarded by confirmation metadata")
			}
			if f.pages.Load() != 3 || f.canonicalNonces.Load() != 1 || f.writes.Load() != 0 {
				t.Fatal("canonical decision skipped full history, repeated a nonce read or wrote")
			}
		})
	}
}

func TestRecoveryStreamingDropsUsedNoncesBeforePendingBudgets(t *testing.T) {
	for _, name := range []string{"all-used", "pending-at-end", "pending-byte-limit", "pending-entry-limit"} {
		t.Run(name, func(t *testing.T) {
			key, err := crypto.GenerateKey()
			if err != nil {
				t.Fatal(err)
			}
			var raws [][]byte
			total := 0
			for nonce := uint64(0); nonce < 32; nonce++ {
				raw := send(t, key, nonce, big.NewInt(1), 1)
				raws = append(raws, raw)
				total += len(raw)
			}
			f := recoveryFixtureFor(t, 130, raws)
			r := f.relayer()
			f.nonce = 31
			r.cfg.Recovery = RecoveryLimits{MaxRawBytes: int64(len(raws[31])), MaxTransactions: 1}
			switch name {
			case "all-used":
				f.nonce = 32
				r.cfg.Recovery.MaxRawBytes = 1
			case "pending-byte-limit":
				r.cfg.Recovery.MaxRawBytes--
			case "pending-entry-limit":
				f.nonce = 30
				r.cfg.Recovery.MaxRawBytes = int64(total)
			}
			if name != "pending-entry-limit" && int64(total) <= r.cfg.Recovery.MaxRawBytes {
				t.Fatal("fixture does not exceed the old retained-byte charge")
			}
			err = r.recoverInFlight(context.Background())
			refused := strings.HasPrefix(name, "pending-") && name != "pending-at-end"
			if refused {
				if err == nil || len(r.byHash) != 0 || len(r.batches) != 0 || r.nPosted != 0 {
					t.Fatal("exhausted pending retention partially installed")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if name == "all-used" {
					if r.nPosted != 0 || len(r.byHash) != 0 || len(r.batches) != 0 {
						t.Fatal("used history consumed pending retention")
					}
				} else {
					hash := canonicalRecoveryHash(t, raws[31])
					entry := r.byHash[hash]
					if r.nPosted != 1 || len(r.byHash) != 1 || entry == nil || !bytes.Equal(entry.raw, raws[31]) {
						t.Fatal("unused tail bytes were not retained exactly")
					}
				}
			}
			if f.canonicalNonces.Load() != 1 || f.writes.Load() != 0 {
				t.Fatal("streaming retention repeated a nonce read or wrote")
			}
		})
	}
}

func TestRecoveryDerivedMalformedContentStillRefuses(t *testing.T) {
	for _, name := range []string{"payload", "transaction", "signature", "chain"} {
		t.Run(name, func(t *testing.T) {
			f := recoveryFixtureFor(t, 130, nil)
			block := f.blocks[1]
			block.ConfirmationDetail = &znn.ConfirmationDetail{MomentumHeight: 1}
			if name == "payload" {
				block.Data = []byte{derive.MsgBatch, 255}
			} else {
				raw := []byte{255}
				if name == "signature" || name == "chain" {
					chain := big.NewInt(testL2Chain)
					if name == "chain" {
						chain.Add(chain, big.NewInt(1))
					}
					to := common.Address{7}
					data := &types.DynamicFeeTx{ChainID: chain, GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2), Gas: 21000, To: &to, Value: big.NewInt(1)}
					tx := types.NewTx(data)
					if name == "chain" {
						key, err := crypto.GenerateKey()
						if err != nil {
							t.Fatal(err)
						}
						tx, err = types.SignNewTx(key, types.LatestSignerForChainID(chain), data)
						if err != nil {
							t.Fatal(err)
						}
					}
					var err error
					raw, err = tx.MarshalBinary()
					if err != nil {
						t.Fatal(err)
					}
				}
				encoded, err := derive.EncodeBatch([][]byte{raw}, derive.BatchZlib)
				if err != nil {
					t.Fatal(err)
				}
				block.Data = encoded
			}
			f.rebuild()
			r := f.relayer()
			if err := r.recoverInFlight(context.Background()); err == nil || len(r.byHash) != 0 || len(r.batches) != 0 || r.nPosted != 0 {
				t.Fatal("derived annotation bypassed identity, signature or payload validation")
			}
			if f.writes.Load() != 0 {
				t.Fatal("malformed historical source caused a write")
			}
		})
	}
}

func TestRecoveryCanonicalNonceFailurePreventsInstallation(t *testing.T) {
	f := recoveryFixtureFor(t, 130, nil)
	f.blocks[1].ConfirmationDetail = &znn.ConfirmationDetail{MomentumHeight: 1}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
		if err != nil {
			t.Error("owned execution request unavailable")
			return
		}
		var in recoveryRequest
		if len(bytes.TrimSpace(raw)) > 0 && bytes.TrimSpace(raw)[0] == '{' {
			if json.Unmarshal(raw, &in) != nil {
				t.Error("owned execution request refused")
				return
			}
		}
		if in.Method == "eth_getTransactionCount" {
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": in.ID, "error": map[string]any{"code": -32000, "message": "private provider diagnostic"}})
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(raw))
		f.serveEth(w, req)
	}))
	defer server.Close()
	eth, err := ethclient.Dial(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer eth.Close()
	r := f.relayer()
	r.cfg.Eth = eth
	err = r.recoverInFlight(context.Background())
	if err == nil || !strings.Contains(err.Error(), "canonical nonce unavailable") || strings.Contains(err.Error(), "private") || len(r.byHash) != 0 || len(r.batches) != 0 || r.nPosted != 0 || f.writes.Load() != 0 {
		t.Fatal("canonical nonce failure leaked diagnostics or installed source metadata")
	}
}

func TestRecoveryUsedHistoricalSendersRemainBounded(t *testing.T) {
	var raws [][]byte
	for i := 0; i < 2; i++ {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		raws = append(raws, send(t, key, 0, big.NewInt(1), 1))
	}
	f := recoveryFixtureFor(t, 130, raws)
	f.nonce = 1
	f.blocks[1].ConfirmationDetail = &znn.ConfirmationDetail{MomentumHeight: 1}
	r := f.relayer()
	r.cfg.Recovery = RecoveryLimits{MaxSenders: 1, MaxRawBytes: 1, MaxTransactions: 1}
	err := r.recoverInFlight(context.Background())
	if err == nil || !strings.Contains(err.Error(), "sender limit") || f.canonicalNonces.Load() != 1 || len(r.byHash) != 0 || len(r.batches) != 0 || f.writes.Load() != 0 {
		t.Fatal("historical canonical nonce cache bypassed the selected sender limit")
	}
}

func TestRecoveryCanonicalNonceCacheSpansPagesAndBatches(t *testing.T) {
	for _, name := range []string{"zero-pending", "used-prefix"} {
		t.Run(name, func(t *testing.T) {
			key, err := crypto.GenerateKey()
			if err != nil {
				t.Fatal(err)
			}
			var raws [][]byte
			total := 0
			for nonce := uint64(0); nonce < 96; nonce++ {
				raw := send(t, key, nonce, big.NewInt(1), 1)
				raws = append(raws, raw)
				total += len(raw)
			}
			f := recoveryFixtureFor(t, 130, raws)
			for i, index := range []int{1, 64, 128} {
				block := f.blocks[index]
				block.ToAddress = rollup.Inbox
				block.Data, err = derive.EncodeBatch(raws[i*32:(i+1)*32], derive.BatchZlib)
				if err != nil {
					t.Fatal(err)
				}
				if i != 1 {
					block.ConfirmationDetail = &znn.ConfirmationDetail{MomentumHeight: 1}
				}
			}
			f.rebuild()
			r := f.relayer()
			want := 96
			r.cfg.Recovery = RecoveryLimits{MaxRawBytes: int64(total), MaxTransactions: 96, MaxSenders: 1}
			if name == "used-prefix" {
				f.nonce = 95
				want = 1
				r.cfg.Recovery.MaxRawBytes = int64(len(raws[95]))
				r.cfg.Recovery.MaxTransactions = 1
			}
			if err := r.recoverInFlight(context.Background()); err != nil {
				t.Fatal(err)
			}
			if r.nPosted != want || len(r.byHash) != want || f.canonicalNonces.Load() != 1 || f.pages.Load() != 3 || f.writes.Load() != 0 {
				t.Fatal("cross-page canonical cache or pending retention diverged")
			}
			for _, raw := range raws[96-want:] {
				entry := r.byHash[canonicalRecoveryHash(t, raw)]
				if entry == nil || !bytes.Equal(entry.raw, raw) {
					t.Fatal("cross-page retained bytes changed")
				}
			}
		})
	}
}
