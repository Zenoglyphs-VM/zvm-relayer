package relayer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/Zenoglyphs-VM/zvm-core/derive"
	"github.com/Zenoglyphs-VM/zvm-core/devnet"
	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
)

type recoveryFixture struct {
	t               *testing.T
	blocks          []*znn.AccountBlock
	raws            [][]byte
	key             *ecdsa.PrivateKey
	address         znn.Address
	l1              *znn.Client
	eth             *ethclient.Client
	header          *types.Header
	mode            string
	nonce           uint64
	pages           atomic.Int32
	frontierReads   atomic.Int32
	canonicalNonces atomic.Int32
	writes          atomic.Int32
}

func recoveryFixtureFor(t *testing.T, height int, raws [][]byte) *recoveryFixture {
	t.Helper()
	var key *ecdsa.PrivateKey
	if raws == nil {
		var err error
		key, err = crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		raws = [][]byte{send(t, key, 0, zvm(1), 1)}
	}
	f := &recoveryFixture{t: t, raws: raws, key: key, header: &types.Header{Difficulty: big.NewInt(0), Number: big.NewInt(1_000_000), BaseFee: big.NewInt(1), GasLimit: 30_000_000}}
	f.address = znn.NewAccount(devnet.Key(202), nil, 69).Address()
	payload, err := derive.EncodeBatch(raws, derive.BatchZlib)
	if err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= height; n++ {
		block := &znn.AccountBlock{Version: 1, ChainIdentifier: 69, BlockType: znn.BlockTypeUserSend, Address: f.address, ToAddress: f.address, Height: uint64(n)}
		if n == 2 {
			block.ToAddress = rollup.Inbox
			block.Data = payload
		}
		f.blocks = append(f.blocks, block)
	}
	f.rebuild()
	l1 := httptest.NewServer(http.HandlerFunc(f.serveL1))
	t.Cleanup(l1.Close)
	f.l1 = znn.NewClient(l1.URL)
	eth := httptest.NewServer(http.HandlerFunc(f.serveEth))
	t.Cleanup(eth.Close)
	f.eth, err = ethclient.Dial(eth.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.eth.Close)
	return f
}

func (f *recoveryFixture) rebuild() {
	var previous znn.Hash
	for _, block := range f.blocks {
		block.PreviousHash = previous
		block.Hash = block.ComputeHash()
		previous = block.Hash
	}
}

type recoveryRequest struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

func (f *recoveryFixture) serveL1(w http.ResponseWriter, request *http.Request) {
	var in recoveryRequest
	if json.NewDecoder(request.Body).Decode(&in) != nil {
		f.t.Error("fixture request refused")
		return
	}
	var result any
	switch in.Method {
	case "embedded.plasma.get":
		result = znn.PlasmaInfo{MaxPlasma: 105_000_000, CurrentPlasma: 105_000_000}
	case "ledger.getFrontierAccountBlock":
		reads := f.frontierReads.Add(1)
		if len(f.blocks) > 0 {
			result = f.blocks[len(f.blocks)-1]
		}
		if f.mode == "frontier_changed" && reads > 1 {
			block := *f.blocks[len(f.blocks)-1]
			block.Data = []byte{0xff}
			block.Hash = block.ComputeHash()
			result = &block
		}
	case "ledger.getAccountBlocksByHeight":
		f.pages.Add(1)
		var address znn.Address
		var from, count uint64
		if len(in.Params) != 3 || json.Unmarshal(in.Params[0], &address) != nil || json.Unmarshal(in.Params[1], &from) != nil || json.Unmarshal(in.Params[2], &count) != nil || address != f.address || from < 1 || count > 64 || from-1+count > uint64(len(f.blocks)) {
			f.t.Error("unbounded or wrong recovery page")
			return
		}
		list := append([]*znn.AccountBlock(nil), f.blocks[from-1:from-1+count]...)
		page := &znn.BlockList{Count: uint64(len(f.blocks)), List: list}
		switch f.mode {
		case "short_page":
			page.List = page.List[:len(page.List)-1]
		case "null_page":
			result = nil
		case "page_count_is_length":
			page.Count = uint64(len(page.List))
		case "more_page":
			page.More = true
		case "out_of_order":
			page.List[0], page.List[1] = page.List[1], page.List[0]
		case "wrong_predecessor":
			block := *page.List[0]
			block.PreviousHash = znn.Hash{99}
			block.Hash = block.ComputeHash()
			page.List[0] = &block
		case "wrong_hash":
			block := *page.List[0]
			block.Hash = znn.Hash{99}
			page.List[0] = &block
		case "nil_descendant":
			block := *page.List[0]
			block.DescendantBlocks = []*znn.AccountBlock{nil}
			page.List[0] = &block
		case "oversized_amount":
			block := *page.List[0]
			amount := new(znn.BigInt)
			amount.Int.Lsh(big.NewInt(1), 257)
			block.Amount = amount
			page.List[0] = &block
		case "negative_amount":
			block := *page.List[0]
			amount := new(znn.BigInt)
			amount.Int.SetInt64(-1)
			block.Amount = amount
			page.List[0] = &block
		case "wrong_address":
			block := *page.List[0]
			block.Address = znn.Address{99}
			block.Hash = block.ComputeHash()
			page.List[0] = &block
		case "wrong_chain":
			block := *page.List[0]
			block.ChainIdentifier = 70
			block.Hash = block.ComputeHash()
			page.List[0] = &block
		case "stalled":
			<-request.Context().Done()
			return
		}
		if f.mode != "null_page" {
			result = page
		}
	case "ledger.getAccountBlockByHash":
		var hash znn.Hash
		if len(in.Params) != 1 || json.Unmarshal(in.Params[0], &hash) != nil {
			f.t.Error("wrong chunk request")
			return
		}
		if f.mode != "missing_chunk" {
			for _, block := range f.blocks {
				if block.Hash == hash {
					result = block
				}
			}
		}
	default:
		f.writes.Add(1)
		f.t.Errorf("unexpected or write recovery RPC %s", in.Method)
		return
	}
	envelope := map[string]any{"jsonrpc": "2.0", "id": in.ID, "result": result}
	switch f.mode {
	case "wrong_response_id":
		envelope["id"] = 99999
	case "remote_error":
		delete(envelope, "result")
		envelope["error"] = map[string]any{"code": -1, "message": "private fixture diagnostic"}
	case "missing_result":
		delete(envelope, "result")
	case "oversized_response":
		envelope["padding"] = strings.Repeat("x", 1<<20)
	case "duplicate_envelope":
		raw, _ := json.Marshal(envelope)
		fmt.Fprintf(w, "{\"id\":0,%s", raw[1:])
		return
	}
	_ = json.NewEncoder(w).Encode(envelope)
}

func (f *recoveryFixture) ethReply(in recoveryRequest) map[string]any {
	var result any
	switch in.Method {
	case "eth_chainId":
		result = hexutil.Uint64(testL2Chain)
		if f.mode == "wrong_execution_chain" {
			result = hexutil.Uint64(testL2Chain + 1)
		}
	case "eth_getBlockByNumber":
		result = f.header
		if f.mode == "execution_reorg" && len(in.Params) > 0 && string(in.Params[0]) != `"latest"` {
			header := *f.header
			header.Extra = []byte{1}
			result = &header
		}
	case "eth_getTransactionCount":
		if len(in.Params) != 2 {
			f.t.Error("missing nonce reference")
		} else if bytes.HasPrefix(in.Params[1], []byte("{")) {
			var selector struct {
				BlockHash        common.Hash `json:"blockHash"`
				RequireCanonical bool        `json:"requireCanonical"`
			}
			if json.Unmarshal(in.Params[1], &selector) != nil || selector.BlockHash != f.header.Hash() || !selector.RequireCanonical {
				f.t.Error("recovery nonce is not canonical hash-selected")
			}
			f.canonicalNonces.Add(1)
		}
		result = hexutil.Uint64(f.nonce)
	case "eth_getBalance":
		result = (*hexutil.Big)(new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil))
	default:
		f.writes.Add(1)
		f.t.Errorf("unexpected or write execution RPC %s", in.Method)
	}
	return map[string]any{"jsonrpc": "2.0", "id": in.ID, "result": result}
}

func (f *recoveryFixture) serveEth(w http.ResponseWriter, request *http.Request) {
	var raw json.RawMessage
	if json.NewDecoder(request.Body).Decode(&raw) != nil {
		f.t.Error("invalid execution request")
		return
	}
	if len(raw) > 0 && raw[0] == '[' {
		var requests []recoveryRequest
		_ = json.Unmarshal(raw, &requests)
		var results []map[string]any
		for _, in := range requests {
			results = append(results, f.ethReply(in))
		}
		_ = json.NewEncoder(w).Encode(results)
		return
	}
	var in recoveryRequest
	_ = json.Unmarshal(raw, &in)
	_ = json.NewEncoder(w).Encode(f.ethReply(in))
}

func (f *recoveryFixture) relayer() *Relayer {
	r := submitting(testRelayer(f.eth))
	r.cfg.Rollup.L1ChainID = 69
	r.cfg.L1 = f.l1
	r.cfg.Accounts = []*znn.Account{znn.NewAccount(devnet.Key(202), f.l1, 69)}
	r.capacity = []uint64{7}
	return r
}

func TestCompleteRecoveryReachesBeyondTheOldSuffix(t *testing.T) {
	f := recoveryFixtureFor(t, 130, nil)
	r := f.relayer()
	if err := r.recoverInFlight(context.Background()); err != nil {
		t.Fatal(err)
	}
	tx := new(types.Transaction)
	_ = tx.UnmarshalBinary(f.raws[0])
	entry := r.byHash[tx.Hash()]
	if f.pages.Load() != 3 || f.frontierReads.Load() != 2 || f.canonicalNonces.Load() != 1 || r.nPosted != 1 || len(r.batches) != 1 || entry == nil || !bytes.Equal(entry.raw, f.raws[0]) || r.batches[0].height != 2 || r.nQueued != 0 || f.writes.Load() != 0 {
		t.Fatal("complete history or exact pending bytes were not adopted")
	}
	if got, err := r.Submit(context.Background(), f.raws[0]); err != nil || got != tx.Hash() || r.nQueued != 0 {
		t.Fatal("identical retry became a new submission")
	}
}

func TestCompleteRecoveryRefusalsLeaveAllStateUntouched(t *testing.T) {
	cases := []struct {
		name, mode string
		limits     RecoveryLimits
		prepare    func(*recoveryFixture, *Relayer)
	}{
		{name: "short_page", mode: "short_page"}, {name: "null_page", mode: "null_page"}, {name: "total_count_is_not_page_length", mode: "page_count_is_length"},
		{name: "ambiguous_more", mode: "more_page"}, {name: "out_of_order", mode: "out_of_order"}, {name: "wrong_predecessor", mode: "wrong_predecessor"},
		{name: "wrong_hash", mode: "wrong_hash"}, {name: "nil_descendant", mode: "nil_descendant"}, {name: "oversized_amount", mode: "oversized_amount"}, {name: "negative_amount", mode: "negative_amount"}, {name: "wrong_address", mode: "wrong_address"}, {name: "wrong_chain", mode: "wrong_chain"},
		{name: "changed_frontier", mode: "frontier_changed"}, {name: "execution_reorg", mode: "execution_reorg"}, {name: "wrong_execution_chain", mode: "wrong_execution_chain"},
		{name: "wrong_response_id", mode: "wrong_response_id"}, {name: "remote_error_is_private", mode: "remote_error"}, {name: "missing_result", mode: "missing_result"},
		{name: "duplicate_envelope", mode: "duplicate_envelope"}, {name: "response_cap", mode: "oversized_response", limits: RecoveryLimits{MaxResponseBytes: 1024}},
		{name: "total_response_cap", prepare: func(f *recoveryFixture, r *Relayer) {
			results := []any{znn.PlasmaInfo{MaxPlasma: 105_000_000, CurrentPlasma: 105_000_000}, f.blocks[len(f.blocks)-1]}
			for from := 0; from < len(f.blocks); from += 64 {
				end := min(from+64, len(f.blocks))
				results = append(results, znn.BlockList{Count: uint64(len(f.blocks)), List: f.blocks[from:end]})
			}
			largest := int64(1024)
			for _, result := range results {
				raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 8192, "result": result})
				largest = max(largest, int64(len(raw))+64)
			}
			// Every individual fixture response fits. Only cumulative bytes
			// exhaust this budget after earlier complete responses.
			r.cfg.Recovery.MaxResponseBytes = largest
			r.cfg.Recovery.MaxTotalResponseBytes = largest
		}},
		{name: "whole_history_block_limit", limits: RecoveryLimits{MaxBlocks: 64}}, {name: "RPC_limit", limits: RecoveryLimits{MaxCalls: 1}},
		{name: "raw_byte_limit", limits: RecoveryLimits{MaxRawBytes: 1}}, {name: "invalid_limits", limits: RecoveryLimits{MaxCalls: -1}},
		{name: "deadline", mode: "stalled", limits: RecoveryLimits{Timeout: 20 * time.Millisecond}},
		{name: "transaction_count_limit", limits: RecoveryLimits{MaxTransactions: 1}, prepare: func(f *recoveryFixture, r *Relayer) {
			key, _ := crypto.GenerateKey()
			payload, _ := derive.EncodeBatch([][]byte{send(f.t, key, 0, zvm(1), 1), send(f.t, key, 1, zvm(1), 1)}, derive.BatchZlib)
			f.blocks[1].Data = payload
			f.rebuild()
		}},
		{name: "sender_limit", limits: RecoveryLimits{MaxSenders: 1}, prepare: func(f *recoveryFixture, r *Relayer) {
			a, _ := crypto.GenerateKey()
			b, _ := crypto.GenerateKey()
			payload, _ := derive.EncodeBatch([][]byte{send(f.t, a, 0, zvm(1), 1), send(f.t, b, 0, zvm(1), 1)}, derive.BatchZlib)
			f.blocks[1].Data = payload
			f.rebuild()
		}},
		{name: "invalid_batch", prepare: func(f *recoveryFixture, r *Relayer) { f.blocks[1].Data = []byte{derive.MsgBatch, 255}; f.rebuild() }},
		{name: "duplicate_pending_hash", prepare: func(f *recoveryFixture, r *Relayer) {
			f.blocks[2].ToAddress = rollup.Inbox
			f.blocks[2].Data = f.blocks[1].Data
			f.rebuild()
		}},
		{name: "conflicting_pending_nonce", prepare: func(f *recoveryFixture, r *Relayer) {
			payload, _ := derive.EncodeBatch([][]byte{send(f.t, f.key, 0, zvm(1), 5)}, derive.BatchZlib)
			f.blocks[2].ToAddress = rollup.Inbox
			f.blocks[2].Data = payload
			f.rebuild()
		}},
		{name: "duplicate_account", prepare: func(f *recoveryFixture, r *Relayer) { r.cfg.Accounts = append(r.cfg.Accounts, r.cfg.Accounts[0]) }},
		{name: "existing_live_lane", prepare: func(f *recoveryFixture, r *Relayer) { r.byHash[common.Hash{9}] = &entry{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := recoveryFixtureFor(t, 130, nil)
			f.mode = tc.mode
			r := f.relayer()
			r.cfg.Recovery = tc.limits
			if tc.prepare != nil {
				tc.prepare(f, r)
			}
			before := r.Snapshot()
			beforeHashes := len(r.byHash)
			beforeCapacity := append([]uint64(nil), r.capacity...)
			err := r.recoverInFlight(context.Background())
			if err == nil || strings.Contains(err.Error(), "private fixture") || !reflect.DeepEqual(before, r.Snapshot()) || len(r.byHash) != beforeHashes || !reflect.DeepEqual(r.capacity, beforeCapacity) || f.writes.Load() != 0 {
				t.Fatalf("recovery refusal mutated state or exposed private diagnostics: %v", err)
			}
		})
	}
}

func TestCompleteRecoveryChecksAssemblyAncestryAndBytes(t *testing.T) {
	for _, name := range []string{"valid", "missing_chunk", "unknown_ancestor", "assembly_byte_limit"} {
		t.Run(name, func(t *testing.T) {
			f := recoveryFixtureFor(t, 130, nil)
			payload := f.blocks[1].Data
			cut := len(payload) / 2
			f.blocks[1].Data = append([]byte{derive.MsgChunk}, payload[:cut]...)
			f.blocks[2].ToAddress = rollup.Inbox
			f.blocks[2].Data = append([]byte{derive.MsgChunk}, payload[cut:]...)
			f.rebuild()
			hashes := []znn.Hash{f.blocks[1].Hash, f.blocks[2].Hash}
			if name == "unknown_ancestor" {
				hashes[0] = znn.Hash{77}
			}
			f.blocks[3].ToAddress = rollup.Inbox
			f.blocks[3].Data = derive.EncodeAssemble(hashes)
			f.rebuild()
			r := f.relayer()
			if name == "missing_chunk" {
				f.mode = "missing_chunk"
			}
			if name == "assembly_byte_limit" {
				r.cfg.Rollup.MaxAssembledBytes = 4
			}
			err := r.recoverInFlight(context.Background())
			if name == "valid" {
				if err != nil || r.nPosted != 1 || len(r.batches) != 1 || r.batches[0].height != 4 {
					t.Fatalf("complete assembly failed: %v", err)
				}
			} else if err == nil || r.nPosted != 0 || len(r.byHash) != 0 {
				t.Fatal("ambiguous assembly admitted partial state")
			}
			if f.writes.Load() != 0 {
				t.Fatal("recovery wrote to a configured chain")
			}
		})
	}
}

func TestFailedRecoveryUnblocksAdmissionWithoutReadiness(t *testing.T) {
	f := recoveryFixtureFor(t, 130, nil)
	f.mode = "short_page"
	r := f.relayer()
	r.adopted = make(chan struct{})
	finished := make(chan error, 1)
	go func() { _, err := r.Submit(context.Background(), f.raws[0]); finished <- err }()
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("partial recovery started workers")
	}
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("submission acknowledged after failed recovery")
		}
	case <-time.After(time.Second):
		t.Fatal("submission waiter did not join")
	}
	if _, err := r.NextNonce(context.Background(), common.Address{}); err == nil {
		t.Fatal("pending nonce escaped failed readiness")
	}
	status := r.Snapshot()
	if status.Ready || status.RecoveryStatus != "failed" || status.AdmissionMode != "volatile" || status.AdmissionDurable || r.nPosted != 0 || r.nQueued != 0 {
		t.Fatal("failed recovery presented an admission promise")
	}
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("the same service lifecycle was restarted")
	}
}

func TestVolatileAdmissionOwnsItsAcknowledgedBytes(t *testing.T) {
	f := recoveryFixtureFor(t, 0, nil)
	r := f.relayer()
	raw := append([]byte(nil), f.raws[0]...)
	original := append([]byte(nil), raw...)
	hash, err := r.Submit(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0
	}
	if !bytes.Equal(r.byHash[hash].raw, original) || r.Snapshot().AdmissionDurable || r.Snapshot().AdmissionMode != "volatile" {
		t.Fatal("acknowledged volatile identity changed with its caller buffer")
	}
}
