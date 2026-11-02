package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Zenoglyphs-VM/zvm-core/derive"
	"github.com/Zenoglyphs-VM/zvm-core/devnet"
	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// Only this owned loopback fixture accepts a publication. Original recovery
// fixtures and their zero-write oracles remain unchanged. The new block is
// authored by the real production CLI using the public development NoM key.
type durablePublicationFixture struct {
	base                               *recoveryProcessFixture
	server                             *httptest.Server
	mode                               string
	higher                             []byte
	mu                                 sync.Mutex
	blocks                             []*znn.AccountBlock
	attempts, verified                 int
	entered, release, finished         chan struct{}
	enterOnce, releaseOnce, finishOnce sync.Once
}

func newDurablePublicationFixture(t *testing.T, mode string) *durablePublicationFixture {
	t.Helper()
	base := newRecoveryProcessFixture(t, "volatile")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sign := func(tip int64) []byte {
		to := common.Address{7}
		tx, err := types.SignNewTx(key, types.LatestSignerForChainID(big.NewInt(recoveryProcessChain)), &types.DynamicFeeTx{
			ChainID: big.NewInt(recoveryProcessChain), Nonce: 0, Gas: 21000, GasFeeCap: big.NewInt(tip + 1), GasTipCap: big.NewInt(tip), To: &to, Value: big.NewInt(1)})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := tx.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	base.raw = sign(1)
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(base.raw); err != nil {
		t.Fatal(err)
	}
	base.hash = tx.Hash()
	f := &durablePublicationFixture{base: base, mode: mode, higher: sign(5), blocks: append([]*znn.AccountBlock(nil), base.blocks...),
		entered: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(f.nom))
	t.Cleanup(func() { f.releaseOnce.Do(func() { close(f.release) }); f.server.Close() })
	return f
}

func (f *durablePublicationFixture) nom(w http.ResponseWriter, request *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(request.Body, (32<<10)+1))
	if err != nil || len(raw) > 32<<10 {
		f.base.t.Error("owned publication request limit refused")
		return
	}
	var in recoveryProcessRequest
	if json.Unmarshal(raw, &in) != nil {
		f.base.t.Error("owned publication request refused")
		return
	}
	var result any
	switch in.Method {
	case "ledger.publishRawTransaction":
		f.mu.Lock()
		f.attempts++
		var block znn.AccountBlock
		valid := len(in.Params) == 1 && json.Unmarshal(in.Params[0], &block) == nil
		last := f.blocks[len(f.blocks)-1]
		public := devnet.Key(202).Public
		valid = valid && block.Version == 1 && block.ChainIdentifier == 69 && block.BlockType == znn.BlockTypeUserSend &&
			block.Address == last.Address && block.ToAddress == rollup.Inbox && block.Height == last.Height+1 && block.PreviousHash == last.Hash &&
			block.MomentumAcknowledged.Height == 10 && block.Amount != nil && block.AmountBig().Sign() == 0 &&
			block.TokenStandard == znn.ZeroTokenStandard && len(block.DescendantBlocks) == 0 &&
			bytes.Equal(block.PublicKey, public) && block.ComputeHash() == block.Hash && ed25519.Verify(public, block.Hash[:], block.Signature)
		if valid {
			valid = len(block.Data) > 1 && block.Data[0] == derive.MsgBatch
			if valid {
				payloads, decodeErr := derive.DecodeBatch(block.Data[1:], 1<<20, false)
				valid = decodeErr == nil && len(payloads) == 1 && bytes.Equal(payloads[0], f.base.raw)
			}
		}
		if !valid || f.attempts != 1 {
			f.mu.Unlock()
			f.base.t.Error("actual production publication changed signature, chain, payload or write count")
			return
		}
		f.verified++
		if f.mode != "unaccepted" {
			f.blocks = append(f.blocks, &block)
		}
		f.mu.Unlock()
		f.enterOnce.Do(func() { close(f.entered) })
		defer f.finishOnce.Do(func() { close(f.finished) })
		if f.mode != "acknowledged" {
			select {
			case <-f.release:
			case <-request.Context().Done():
				return
			}
		}
	case "ledger.getFrontierAccountBlock":
		f.base.frontiers.Add(1)
		f.mu.Lock()
		result = f.blocks[len(f.blocks)-1]
		f.mu.Unlock()
	case "ledger.getAccountBlocksByHeight":
		f.base.pages.Add(1)
		var from, count uint64
		f.mu.Lock()
		if len(in.Params) != 3 || json.Unmarshal(in.Params[1], &from) != nil || json.Unmarshal(in.Params[2], &count) != nil || from < 1 || count > 64 || from-1+count > uint64(len(f.blocks)) {
			f.mu.Unlock()
			f.base.t.Error("owned publication history page refused")
			return
		}
		result = znn.BlockList{Count: uint64(len(f.blocks)), List: append([]*znn.AccountBlock(nil), f.blocks[from-1:from-1+count]...)}
		f.mu.Unlock()
	case "ledger.getAccountBlockByHash":
		var hash znn.Hash
		if len(in.Params) != 1 || json.Unmarshal(in.Params[0], &hash) != nil {
			f.base.t.Error("owned publication hash request refused")
			return
		}
		f.mu.Lock()
		for _, block := range f.blocks {
			if block.Hash == hash {
				result = block
			}
		}
		f.mu.Unlock()
	case "ledger.getUnconfirmedBlocksByAddress":
		f.mu.Lock()
		var pending []*znn.AccountBlock
		if len(f.blocks) > 130 {
			pending = append(pending, f.blocks[130:]...)
		}
		result = znn.BlockList{Count: uint64(len(pending)), List: pending}
		f.mu.Unlock()
	default:
		request.Body = io.NopCloser(bytes.NewReader(raw))
		f.base.nom(w, request)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": in.ID, "result": result})
}

func (f *durablePublicationFixture) start(binary, directory string) *recoveryOwnedProcess {
	return f.base.startWithRecoveryFlags(binary, "65536", "-l1", f.server.URL, "-max-wait", "1ms", "-admission-store", directory)
}

func durablePublicationReplacementRefused(t *testing.T, p *recoveryOwnedProcess, raw []byte) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 2, "method": "eth_sendRawTransaction", "params": []string{hexutil.Encode(raw)}})
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Post(p.url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal("replacement response unavailable")
	}
	defer response.Body.Close()
	var reply struct {
		Result json.RawMessage
		Error  *rpcError
	}
	if json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&reply) != nil || reply.Error == nil || reply.Error.Code != -32000 || !strings.Contains(reply.Error.Message, "being published") || len(reply.Result) != 0 {
		t.Fatal("actual publication reservation accepted a replacement")
	}
}

func durablePublicationWaitPosted(t *testing.T, p *recoveryOwnedProcess) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		status := recoveryProcessSnapshot(t, p)
		if status.Ready && status.AdmissionDurable && status.AdmissionMode == "local-sync" && status.Queued == 0 && status.PostedPending == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("actual acknowledged publication did not reach posted state")
}

func TestDurablePublicationCLIProcesses(t *testing.T) {
	binary := os.Getenv("ZVM_RELAYER_RECOVERY_BINARY")
	if binary == "" {
		t.Skip("requires the declared actual race-instrumented production CLI job")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("production CLI binary must be explicit")
	}
	for _, mode := range []string{"unaccepted", "accepted", "acknowledged"} {
		t.Run(mode, func(t *testing.T) {
			f := newDurablePublicationFixture(t, mode)
			original := bytes.Clone(f.base.raw)
			directory := filepath.Join(t.TempDir(), "admissions")
			first := f.start(binary, directory)
			recoveryHTTPStatus(t, first.url+"/health", 200)
			if recoveryProcessSubmit(t, first, original) != f.base.hash {
				t.Fatal("original durable acknowledgement changed")
			}
			select {
			case <-f.entered:
			case <-time.After(6 * time.Second):
				t.Fatal("real CLI did not enter owned signed publication")
			}
			phase := byte(1)
			if mode == "acknowledged" {
				durablePublicationWaitPosted(t, first)
				phase = 2
			} else {
				durablePublicationReplacementRefused(t, first, f.higher)
				if recoveryProcessSubmit(t, first, original) != f.base.hash {
					t.Fatal("reserved original identity changed")
				}
			}
			first.kill(t)
			select {
			case <-f.finished:
			case <-time.After(3 * time.Second):
				t.Fatal("owned publication handler did not stop")
			}
			durableCLIStoredWire(t, directory, f.base.hash, original, phase)
			second := f.start(binary, directory)
			if mode == "unaccepted" {
				if second.wait(t) == nil {
					t.Fatal("unresolved publication was blindly restarted")
				}
				second.output.mu.Lock()
				diagnostic := second.output.buffer.String()
				second.output.mu.Unlock()
				if !strings.Contains(diagnostic, "durable admission publication outcome unresolved") {
					t.Fatal("restart did not refuse the actual unresolved publication")
				}
				durableCLIStoredWire(t, directory, f.base.hash, original, 1)
			} else {
				recoveryHTTPStatus(t, second.url+"/health", 200)
				durablePublicationWaitPosted(t, second)
				if recoveryProcessSubmit(t, second, original) != f.base.hash {
					t.Fatal("published original hash did not remain idempotent")
				}
				second.kill(t)
				durableCLIStoredWire(t, directory, f.base.hash, original, 2)
			}
			f.mu.Lock()
			attempts, verified, blocks := f.attempts, f.verified, len(f.blocks)
			f.mu.Unlock()
			wantBlocks := 131
			if mode == "unaccepted" {
				wantBlocks = 130
			}
			if attempts != 1 || verified != 1 || blocks != wantBlocks || f.base.writes.Load() != 0 || f.base.pages.Load() != 6 || !bytes.Equal(original, f.base.raw) {
				t.Fatal("publication restart repeated a write or lost complete exact-byte history")
			}
			forcedStops := 2
			if mode == "unaccepted" {
				forcedStops = 1
			}
			record := map[string]any{"case": mode, "process_lifetimes": 2, "processes_joined": 2, "forced_stops": forcedStops,
				"controlled_NoM_write_RPCs": 1, "verified_signed_publications": 1, "retained_account_blocks": wantBlocks, "complete_recovery_pages": 6,
				"actual_retained_signed_bytes_equal": true, "original_hash_preserved": true, "blind_republication": false,
				"unresolved_restart_refused": mode == "unaccepted", "posted_restored": mode != "unaccepted", "concurrent_replacement_refused": mode != "acknowledged"}
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			t.Log("DURABLE-PUBLICATION-CLI " + string(raw))
		})
	}
}
