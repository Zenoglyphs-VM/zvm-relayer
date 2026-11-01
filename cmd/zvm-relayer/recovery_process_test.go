package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
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

const recoveryProcessChain = 7340469

type recoveryProcessRequest struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}
type recoveryProcessFixture struct {
	t                        *testing.T
	mode                     string
	blocks                   []*znn.AccountBlock
	header                   *types.Header
	raw                      []byte
	hash                     common.Hash
	l1, eth                  *httptest.Server
	frontiers                atomic.Int32
	pages                    atomic.Int32
	writes                   atomic.Int32
	pageEntered              chan struct{}
	releasePage              chan struct{}
	enteredOnce, releaseOnce sync.Once
}

func newRecoveryProcessFixture(t *testing.T, mode string) *recoveryProcessFixture {
	t.Helper()
	f := &recoveryProcessFixture{t: t, mode: mode, header: &types.Header{Difficulty: big.NewInt(0), Number: big.NewInt(100), BaseFee: big.NewInt(1), GasLimit: 30_000_000}, pageEntered: make(chan struct{}), releasePage: make(chan struct{})}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	to := common.Address{7}
	nonce := uint64(0)
	if mode == "volatile" {
		nonce = 9
	}
	tx, err := types.SignNewTx(key, types.LatestSignerForChainID(big.NewInt(recoveryProcessChain)), &types.DynamicFeeTx{ChainID: big.NewInt(recoveryProcessChain), Nonce: nonce, Gas: 21000, GasFeeCap: big.NewInt(2), GasTipCap: big.NewInt(1), To: &to, Value: big.NewInt(1)})
	if err != nil {
		t.Fatal(err)
	}
	f.raw, err = tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	f.hash = tx.Hash()
	payload, err := derive.EncodeBatch([][]byte{f.raw}, derive.BatchZlib)
	if err != nil {
		t.Fatal(err)
	}
	address := znn.NewAccount(devnet.Key(202), nil, 69).Address()
	var previous znn.Hash
	for height := uint64(1); height <= 130; height++ {
		block := &znn.AccountBlock{Version: 1, ChainIdentifier: 69, BlockType: znn.BlockTypeUserSend, Address: address, ToAddress: address, Height: height, PreviousHash: previous}
		if height == 2 && mode != "volatile" {
			block.ToAddress = rollup.Inbox
			block.Data = payload
		}
		block.Hash = block.ComputeHash()
		previous = block.Hash
		f.blocks = append(f.blocks, block)
	}
	f.l1 = httptest.NewServer(http.HandlerFunc(f.nom))
	f.eth = httptest.NewServer(http.HandlerFunc(f.execution))
	t.Cleanup(func() { f.releaseOnce.Do(func() { close(f.releasePage) }); f.l1.Close(); f.eth.Close() })
	return f
}

func (f *recoveryProcessFixture) nom(w http.ResponseWriter, request *http.Request) {
	var in recoveryProcessRequest
	if json.NewDecoder(request.Body).Decode(&in) != nil {
		f.t.Error("invalid NoM fixture request")
		return
	}
	var result any
	switch in.Method {
	case "embedded.plasma.get":
		result = znn.PlasmaInfo{CurrentPlasma: 105_000_000, MaxPlasma: 105_000_000}
	case "ledger.getFrontierMomentum":
		result = znn.Momentum{Height: 10, Version: 1}
	case "ledger.getFrontierAccountBlock":
		read := f.frontiers.Add(1)
		result = f.blocks[len(f.blocks)-1]
		if f.mode == "frontier" && read > 1 {
			block := *f.blocks[len(f.blocks)-1]
			block.Data = []byte{0xff}
			block.Hash = block.ComputeHash()
			result = &block
		}
	case "ledger.getAccountBlocksByHeight":
		f.pages.Add(1)
		f.enteredOnce.Do(func() { close(f.pageEntered) })
		if f.mode == "incomplete" {
			select {
			case <-f.releasePage:
			case <-request.Context().Done():
				return
			}
		}
		var from, count uint64
		if len(in.Params) != 3 || json.Unmarshal(in.Params[1], &from) != nil || json.Unmarshal(in.Params[2], &count) != nil || from < 1 || count > 64 || from-1+count > uint64(len(f.blocks)) {
			f.t.Error("wrong process recovery page")
			return
		}
		list := append([]*znn.AccountBlock(nil), f.blocks[from-1:from-1+count]...)
		if f.mode == "incomplete" {
			list = list[:len(list)-1]
		}
		result = znn.BlockList{Count: uint64(len(f.blocks)), List: list}
	case "ledger.getAccountBlockByHash":
		var hash znn.Hash
		if len(in.Params) != 1 || json.Unmarshal(in.Params[0], &hash) != nil {
			f.t.Error("invalid process hash query")
			return
		}
		for _, block := range f.blocks {
			if block.Hash == hash {
				result = block
			}
		}
	default:
		f.writes.Add(1)
		f.t.Errorf("unexpected or write process NoM RPC %s", in.Method)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": in.ID, "result": result})
}

func (f *recoveryProcessFixture) executionReply(in recoveryProcessRequest) map[string]any {
	var result any
	switch in.Method {
	case "eth_blockNumber":
		result = hexutil.Uint64(100)
	case "eth_chainId":
		result = hexutil.Uint64(recoveryProcessChain)
	case "eth_getBlockByNumber":
		result = f.header
		if f.mode == "reorg" && len(in.Params) > 0 && string(in.Params[0]) != `"latest"` {
			header := *f.header
			header.Extra = []byte{1}
			result = &header
		}
	case "eth_getTransactionCount":
		result = "0x0"
	case "eth_getBalance":
		result = (*hexutil.Big)(new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil))
	default:
		f.writes.Add(1)
		f.t.Errorf("unexpected or write process execution RPC %s", in.Method)
	}
	return map[string]any{"jsonrpc": "2.0", "id": in.ID, "result": result}
}

func (f *recoveryProcessFixture) execution(w http.ResponseWriter, request *http.Request) {
	var raw json.RawMessage
	if json.NewDecoder(request.Body).Decode(&raw) != nil {
		f.t.Error("invalid execution fixture request")
		return
	}
	if len(raw) > 0 && raw[0] == '[' {
		var ins []recoveryProcessRequest
		_ = json.Unmarshal(raw, &ins)
		var replies []map[string]any
		for _, in := range ins {
			replies = append(replies, f.executionReply(in))
		}
		_ = json.NewEncoder(w).Encode(replies)
		return
	}
	var in recoveryProcessRequest
	_ = json.Unmarshal(raw, &in)
	_ = json.NewEncoder(w).Encode(f.executionReply(in))
}

type recoveryCappedOutput struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	exceeded bool
}

func (w *recoveryCappedOutput) Write(raw []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(raw) > 64<<10-w.buffer.Len() {
		w.exceeded = true
		return 0, fmt.Errorf("process output limit exceeded")
	}
	return w.buffer.Write(raw)
}

type recoveryOwnedProcess struct {
	command  *exec.Cmd
	done     chan error
	terminal bool
	output   *recoveryCappedOutput
	url      string
}

func (f *recoveryProcessFixture) start(binary string, blocks string) *recoveryOwnedProcess {
	return f.startWithRecoveryFlags(binary, blocks)
}

func (f *recoveryProcessFixture) startWithRecoveryFlags(binary string, blocks string, recoveryArgs ...string) *recoveryOwnedProcess {
	f.t.Helper()
	dir := f.t.TempDir()
	cfg := &rollup.Config{L1ChainID: 69, L2ChainID: recoveryProcessChain, L1StartHeight: 1, Inbox: rollup.Inbox, GasLimit: 30_000_000, MaxTxGas: 30_000_000, MaxAssembledBytes: 1 << 20, MaxBatchBytes: 4 << 20}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		f.t.Fatal(err)
	}
	config := filepath.Join(dir, "rollup.json")
	if err = os.WriteFile(config, encoded, 0600); err != nil {
		f.t.Fatal(err)
	}
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		f.t.Fatal(err)
	}
	listen := reserved.Addr().String()
	if err = reserved.Close(); err != nil {
		f.t.Fatal(err)
	}
	p := &recoveryOwnedProcess{done: make(chan error, 1), output: &recoveryCappedOutput{}, url: "http://" + listen}
	p.command = exec.Command(binary, "relayer", "-config", config, "-l1", f.l1.URL, "-eth", f.eth.URL, "-listen", listen, "-accounts", "1", "-first-index", "202", "-footprint=false", "-max-wait", "1h", "-recovery-timeout", "5s", "-recovery-max-blocks", blocks)
	p.command.Args = append(p.command.Args, recoveryArgs...)
	p.command.Env = []string{"PATH=" + os.Getenv("PATH"), "LANG=C", "TZ=UTC", "GORACE=halt_on_error=1"}
	p.command.Stdout = p.output
	p.command.Stderr = p.output
	if err = p.command.Start(); err != nil {
		f.t.Fatal(err)
	}
	go func() { p.done <- p.command.Wait() }()
	f.t.Cleanup(func() {
		if !p.terminal {
			_ = p.command.Process.Kill()
			select {
			case <-p.done:
				p.terminal = true
			case <-time.After(3 * time.Second):
				f.t.Error("owned process did not join")
			}
		}
	})
	return p
}

func (p *recoveryOwnedProcess) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-p.done:
		p.terminal = true
		p.output.mu.Lock()
		exceeded := p.output.exceeded
		p.output.mu.Unlock()
		if exceeded {
			t.Fatal("owned output cap exceeded")
		}
		return err
	case <-time.After(8 * time.Second):
		t.Fatal("owned process deadline exceeded")
	}
	return nil
}
func (p *recoveryOwnedProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if p.wait(t) == nil {
		t.Fatal("forced process stop reported success")
	}
}

func recoveryHTTPStatus(t *testing.T, url string, want int) {
	t.Helper()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected HTTP status %d was not observed", want)
}

func recoveryProcessSnapshot(t *testing.T, p *recoveryOwnedProcess) struct {
	Ready                         bool
	RecoveryStatus, AdmissionMode string
	AdmissionDurable              bool
	Queued, PostedPending         int
} {
	t.Helper()
	var out struct {
		Ready                         bool
		RecoveryStatus, AdmissionMode string
		AdmissionDurable              bool
		Queued, PostedPending         int
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(p.url + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&out) != nil {
		t.Fatal("status response unavailable")
	}
	return out
}

func recoveryProcessSubmit(t *testing.T, p *recoveryOwnedProcess, raw []byte) common.Hash {
	t.Helper()
	encoded, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction", "params": []string{hexutil.Encode(raw)}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var reply struct {
		Result common.Hash     `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if json.NewDecoder(response.Body).Decode(&reply) != nil || len(reply.Error) != 0 || reply.Result == (common.Hash{}) {
		t.Fatal("exact signed hash was not acknowledged")
	}
	return reply.Result
}

func TestRecoveryCLIProcesses(t *testing.T) {
	binary := os.Getenv("ZVM_RELAYER_RECOVERY_BINARY")
	if binary == "" {
		t.Skip("requires the declared actual race-instrumented production CLI job")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("production CLI binary must be explicit")
	}
	t.Run("incomplete_history_blocks_readiness", func(t *testing.T) {
		f := newRecoveryProcessFixture(t, "incomplete")
		p := f.start(binary, "65536")
		select {
		case <-f.pageEntered:
		case <-time.After(6 * time.Second):
			t.Fatal("actual process never entered recovery")
		}
		recoveryHTTPStatus(t, p.url+"/health", 503)
		status := recoveryProcessSnapshot(t, p)
		if status.Ready || status.AdmissionDurable || status.AdmissionMode != "volatile" || status.RecoveryStatus != "starting" || status.Queued != 0 || status.PostedPending != 0 {
			t.Fatal("incomplete history was presented as ready")
		}
		f.releaseOnce.Do(func() { close(f.releasePage) })
		if p.wait(t) == nil || f.writes.Load() != 0 {
			t.Fatal("incomplete history started a writer")
		}
		t.Log(`{"case":"incomplete_history","health":503,"process_joined":true,"write_RPCs":0}`)
	})
	for _, tc := range []struct{ name, mode, blocks string }{{"whole_history_limit_refuses_suffix", "budget", "64"}, {"changed_frontier_refuses", "frontier", "65536"}, {"changed_execution_anchor_refuses", "reorg", "65536"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecoveryProcessFixture(t, tc.mode)
			p := f.start(binary, tc.blocks)
			if p.wait(t) == nil || f.writes.Load() != 0 {
				t.Fatal("incomplete or inconsistent recovery started a writer")
			}
			if tc.mode == "budget" && f.pages.Load() != 0 {
				t.Fatal("block-limit refusal scanned a suffix")
			}
			t.Logf(`{"case":%q,"process_joined":true,"write_RPCs":0}`, tc.name)
		})
	}
	t.Run("early_batch_survives_process_kill", func(t *testing.T) {
		f := newRecoveryProcessFixture(t, "pending")
		original := append([]byte(nil), f.raw...)
		for attempt := 0; attempt < 2; attempt++ {
			p := f.start(binary, "65536")
			recoveryHTTPStatus(t, p.url+"/health", 200)
			status := recoveryProcessSnapshot(t, p)
			if !status.Ready || status.RecoveryStatus != "complete" || status.AdmissionDurable || status.AdmissionMode != "volatile" || status.PostedPending != 1 || status.Queued != 0 || recoveryProcessSubmit(t, p, original) != f.hash {
				t.Fatal("early pending batch identity was lost or queued again")
			}
			p.kill(t)
		}
		if !bytes.Equal(f.raw, original) || f.writes.Load() != 0 || f.pages.Load() != 6 {
			t.Fatal("restart changed signed bytes, scanned a suffix or published again")
		}
		t.Log(`{"case":"early_pending_batch","account_blocks":130,"batch_height":2,"process_lifetimes":2,"forced_stops":2,"original_hash_acknowledged_twice":true,"processes_joined":2,"write_RPCs":0}`)
	})
	t.Run("operator_response_byte_budget", func(t *testing.T) { recoveryCLIByteBudget(t, binary) })
	t.Run("queued_ACK_remains_explicitly_volatile", func(t *testing.T) {
		f := newRecoveryProcessFixture(t, "volatile")
		original := append([]byte(nil), f.raw...)
		p := f.start(binary, "65536")
		recoveryHTTPStatus(t, p.url+"/health", 200)
		if recoveryProcessSubmit(t, p, original) != f.hash || recoveryProcessSnapshot(t, p).Queued != 1 {
			t.Fatal("synthetic gapped queue did not acknowledge its exact identity")
		}
		p.kill(t)
		restarted := f.start(binary, "65536")
		recoveryHTTPStatus(t, restarted.url+"/health", 200)
		status := recoveryProcessSnapshot(t, restarted)
		if status.Queued != 0 || status.PostedPending != 0 || status.AdmissionDurable || status.AdmissionMode != "volatile" || !bytes.Equal(original, f.raw) {
			t.Fatal("volatile acknowledgment became a durability promise or changed identity")
		}
		restarted.kill(t)
		if f.writes.Load() != 0 {
			t.Fatal("unresolved volatile transaction was automatically republished")
		}
		t.Log(`{"case":"volatile_queued_ack_crash","process_lifetimes":2,"original_hash_acknowledged":true,"queue_absent_after_restart":true,"client_signed_bytes_retained":true,"automatic_resubmission":false,"processes_joined":2,"write_RPCs":0}`)
	})
}
