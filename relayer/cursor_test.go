package relayer

import (
	"encoding/json"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/Zenoglyphs-VM/zvm-core/znn"
)

// The fleet benchmark's funder declares 16.75 M gas a call, so one block admits
// thirteen of them under the lane's 225 M.
const (
	funderGas = 16_750_000
	laneGas   = 225_000_000
)

// fakeRPC serves JSON-RPC from answer, one request or a batch of them, as
// ethclient sends both. A nil answer is a method the test did not expect.
func fakeRPC(t *testing.T, answer func(method string) any) *ethclient.Client {
	t.Helper()
	type request struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	reply := func(in request) map[string]any {
		result := answer(in.Method)
		if result == nil {
			t.Errorf("unexpected RPC %s", in.Method)
		}
		return map[string]any{"jsonrpc": "2.0", "id": in.ID, "result": result}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var raw json.RawMessage
		json.NewDecoder(req.Body).Decode(&raw)
		if len(raw) > 0 && raw[0] == '[' {
			var ins []request
			json.Unmarshal(raw, &ins)
			outs := make([]map[string]any, len(ins))
			for i, in := range ins {
				outs[i] = reply(in)
			}
			json.NewEncoder(w).Encode(outs)
			return
		}
		var in request
		json.Unmarshal(raw, &in)
		json.NewEncoder(w).Encode(reply(in))
	}))
	t.Cleanup(srv.Close)
	c, err := ethclient.Dial(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// fakeHeader is a block header with just enough in it to decode.
func fakeHeader() map[string]any {
	return map[string]any{
		"number": "0x1", "hash": "0x" + strings.Repeat("11", 32), "parentHash": "0x" + strings.Repeat("00", 32),
		"sha3Uncles": "0x" + strings.Repeat("00", 32), "logsBloom": "0x" + strings.Repeat("00", 256),
		"transactionsRoot": "0x" + strings.Repeat("00", 32), "stateRoot": "0x" + strings.Repeat("00", 32),
		"receiptsRoot": "0x" + strings.Repeat("00", 32), "miner": "0x" + strings.Repeat("00", 20),
		"difficulty": "0x0", "extraData": "0x", "gasLimit": "0x1", "gasUsed": "0x0",
		"timestamp": "0x1", "nonce": "0x0000000000000000", "mixHash": "0x" + strings.Repeat("00", 32),
		"baseFeePerGas": "0x1", "size": "0x0", "totalDifficulty": "0x0", "uncles": []any{}, "transactions": []any{},
	}
}

// fakeChain answers with whatever nonce and balance the test sets, at a head
// that never moves. A nil balance is one no transaction here can exhaust.
func fakeChain(t *testing.T, nonce *atomic.Uint64, balance ...*big.Int) *ethclient.Client {
	t.Helper()
	bal := new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
	if len(balance) > 0 {
		bal = balance[0]
	}
	return fakeRPC(t, func(method string) any {
		switch method {
		case "eth_getTransactionCount":
			return hexutil.Uint64(nonce.Load())
		case "eth_getBalance":
			return (*hexutil.Big)(bal)
		case "eth_getBlockByNumber":
			return fakeHeader()
		case "eth_blockNumber":
			return hexutil.Uint64(1_000_000)
		}
		return nil
	})
}

func testRelayer(eth *ethclient.Client) *Relayer {
	return &Relayer{
		cfg: Config{Eth: eth, MaxBatchGas: laneGas, MaxRequeues: 8, QueueTTL: 5 * time.Minute,
			MaxWait: time.Second, Log: slog.New(slog.DiscardHandler)},
		senders:  map[common.Address]*sender{},
		byHash:   map[common.Hash]*entry{},
		wake:     []chan struct{}{make(chan struct{}, 1)},
		inFlight: []int{0},
		byShard:  []map[common.Address]*sender{{}},
		load:     []int{0},
	}
}

// track registers a sender the way submit does: in the map and in its
// shard's set, which is what ready() walks.
func track(r *Relayer, s *sender) *sender {
	r.senders[s.addr] = s
	r.byShard[s.shard][s.addr] = s
	return s
}

func funderEntry(from common.Address, nonce uint64) *entry {
	tx := types.NewTx(&types.DynamicFeeTx{Nonce: nonce, Gas: funderGas})
	return &entry{tx: tx, raw: make([]byte, 64), from: from, received: time.Now(), queuedAt: time.Now()}
}

// post moves entries from queued to posted as the poster does, and returns
// the batch that carries them.
func post(r *Relayer, s *sender, nonces ...uint64) *batch {
	b := &batch{}
	for _, n := range nonces {
		e := s.queued[n]
		if e == nil {
			e = funderEntry(s.addr, n)
			r.byHash[e.tx.Hash()] = e
		} else {
			delete(s.queued, n)
		}
		s.posted[n] = e
		s.postedGas += e.tx.Gas()
		if n >= s.next {
			s.next = n + 1
		}
		e.state, e.batch = posted, b
		b.entries = append(b.entries, e)
	}
	return b
}

func span(from, to uint64) []uint64 {
	var out []uint64
	for n := from; n <= to; n++ {
		out = append(out, n)
	}
	return out
}

// The wedge measured on 2026-09-16, replayed. The funder has 91–104 in one
// batch and a later batch, 358–366, that landed first while the chain was at
// 91. That later batch settles gapped; then 91–103 are included and 104 is
// refused "block full". Before the fix the cursor was left at 91 — a posted,
// then used, nonce — so 104 was never reachable again, was swept, and every
// resubmission of it went the same way.
func TestAHeadRefusedForSpaceIsPostedAgain(t *testing.T) {
	var chain atomic.Uint64
	chain.Store(91)
	r := testRelayer(fakeChain(t, &chain))
	from := common.HexToAddress("0x6b6656f76c55f2df38df9ad285a49fdeab725e41")
	s := track(r, &sender{addr: from, next: 91, queued: map[uint64]*entry{}, posted: map[uint64]*entry{}})

	head := post(r, s, span(91, 104)...)
	ahead := post(r, s, span(358, 366)...)

	// The later batch is derived first, with the chain still at 91.
	r.settle(t.Context(), ahead, map[common.Hash]bool{}, false)
	if s.next != 105 {
		t.Fatalf("cursor %d after a gapped batch settled; it should skip the 91–104 still in flight and rest at 105", s.next)
	}
	for _, n := range span(358, 366) {
		if e := s.queued[n]; e == nil || e.requeues != 0 {
			t.Fatalf("gapped nonce %d should be kept without spending a retry: %+v", n, e)
		}
	}

	// Then the head batch: 91–103 land, 104 does not.
	chain.Store(104)
	included := map[common.Hash]bool{}
	for _, e := range head.entries[:13] {
		included[e.tx.Hash()] = true
	}
	r.settle(t.Context(), head, included, false)
	if s.next != 104 {
		t.Fatalf("cursor %d, want the account's nonce 104", s.next)
	}
	if s.postedGas != 0 {
		t.Fatalf("nothing is in flight, but %d gas is still counted", s.postedGas)
	}
	e := s.queued[104]
	if e == nil || e.requeues != 1 {
		t.Fatalf("104 failed on its own and should be queued with one retry spent: %+v", e)
	}
	got, _ := r.ready(0, math.MaxUint64)
	if len(got) == 0 || got[0].tx.Nonce() != 104 {
		t.Fatalf("104 is not the next thing posted: %v", nonces(got))
	}
}

// Waiting its turn is not staleness. Before the fix a transaction queued behind
// its own sender's earlier nonces was discarded once QueueTTL passed, which
// opened the gap that stranded the rest.
func TestOnlyAGappedEntryIsSwept(t *testing.T) {
	r := testRelayer(nil)
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	s := track(r, &sender{addr: from, next: 5, queued: map[uint64]*entry{}, posted: map[uint64]*entry{},
		lastUse: time.Now()})
	old := time.Now().Add(-time.Hour)
	add := func(n uint64) {
		e := funderEntry(from, n)
		e.queuedAt = old
		s.queued[n] = e
		r.byHash[e.tx.Hash()] = e
		r.nQueued++
	}
	for _, n := range []uint64{5, 6, 9, 12} {
		add(n)
	}
	// 7 and 8 are in flight, so 9 is behind them rather than behind a gap.
	for _, n := range []uint64{7, 8} {
		e := funderEntry(from, n)
		s.posted[n] = e
	}
	r.sweep()
	for _, n := range []uint64{5, 6, 9} {
		if s.queued[n] == nil {
			t.Errorf("nonce %d is reachable and was swept", n)
		}
	}
	if s.queued[12] != nil {
		t.Error("nonce 12 sits behind a gap at 10 and should have been swept")
	}
}

// A heavy sender never has more than one block's worth in flight, and a light
// one is not held back by it.
func TestOneSenderHoldsOneBlockInFlight(t *testing.T) {
	r := testRelayer(nil)
	heavy := common.HexToAddress("0x2222222222222222222222222222222222222222")
	light := common.HexToAddress("0x3333333333333333333333333333333333333333")
	hs := track(r, &sender{addr: heavy, next: 0, queued: map[uint64]*entry{}, posted: map[uint64]*entry{}})
	ls := track(r, &sender{addr: light, next: 0, queued: map[uint64]*entry{}, posted: map[uint64]*entry{}})
	for n := uint64(0); n < 40; n++ {
		hs.queued[n] = funderEntry(heavy, n)
	}
	small := types.NewTx(&types.DynamicFeeTx{Nonce: 0, Gas: 21_000})
	ls.queued[0] = &entry{tx: small, raw: make([]byte, 64), from: light, received: time.Now().Add(time.Second)}

	// An unbounded lane, so only the per-sender rule can stop the heavy one.
	got, _ := r.ready(0, math.MaxUint64)
	var fromHeavy, fromLight int
	for _, e := range got {
		if e.from == heavy {
			fromHeavy++
		} else {
			fromLight++
		}
	}
	if fromHeavy != laneGas/funderGas {
		t.Fatalf("heavy sender offered %d calls, want the %d one block admits", fromHeavy, laneGas/funderGas)
	}
	if fromLight != 1 {
		t.Fatalf("the light sender was held back: %d", fromLight)
	}

	// With a block's worth already in flight, nothing more of it goes out.
	post(r, hs, span(0, 12)...)
	for _, e := range got {
		if e.from == heavy {
			delete(hs.queued, e.tx.Nonce())
		}
	}
	got, _ = r.ready(0, math.MaxUint64)
	for _, e := range got {
		if e.from == heavy {
			t.Fatalf("posted nonce %d with %d gas already in flight", e.tx.Nonce(), hs.postedGas)
		}
	}
}

func nonces(es []*entry) []uint64 {
	var out []uint64
	for _, e := range es {
		out = append(out, e.tx.Nonce())
	}
	return out
}

// Submissions from different senders must not serialize behind the head
// cache: it holds no lock across an RPC. With the balance map emptied every
// block, 100,000 distinct senders miss it almost every time, and a lock held
// across eth_getBalance turned that into one submission per round trip.
func TestHeadForDoesNotSerializeOnRPC(t *testing.T) {
	const delay = 20 * time.Millisecond
	var calls atomic.Int64
	eth := fakeRPC(t, func(method string) any {
		calls.Add(1)
		switch method {
		case "eth_getBlockByNumber":
			time.Sleep(delay)
			return fakeHeader()
		case "eth_getTransactionCount":
			// Batched with eth_getBalance, which pays the round trip.
			return hexutil.Uint64(0)
		}
		time.Sleep(delay)
		return hexutil.Big(*big.NewInt(1e18))
	})
	r := testRelayer(eth)

	const senders = 40
	start := time.Now()
	var wg sync.WaitGroup
	errs := make([]error, senders)
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var a common.Address
			a[0], a[1] = byte(i), byte(i>>8)
			_, _, errs[i] = r.headFor(t.Context(), a)
		}(i)
	}
	wg.Wait()
	took := time.Since(start)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("sender %d: %v", i, err)
		}
	}
	// Serialized, this is senders × delay; concurrent, a small multiple of one
	// round trip. Half the serial time is a wide margin that still fails the
	// lock-held-across-RPC version.
	if took > senders*delay/2 {
		t.Fatalf("%d concurrent balance reads took %s, serialized would be %s", senders, took, senders*delay)
	}
}

// Assigning a shard to a new sender must not cost more as senders accumulate.
// pickShard used to count every sender's outstanding work, for every shard, on
// the first submission of every sender, holding the relayer's lock: at 100,000
// wallets submitting once each, the lane decayed from the offered 200 tx/s to
// 35 over one phase.
func TestPickShardDoesNotScaleWithSenders(t *testing.T) {
	r := testRelayer(nil)
	r.cfg.Accounts = make([]*znn.Account, 8)
	r.inFlight = make([]int, 8)
	r.load = make([]int, 8)
	r.byShard = make([]map[common.Address]*sender, 8)
	for i := range r.byShard {
		r.byShard[i] = map[common.Address]*sender{}
	}
	pick := func(n int) time.Duration {
		start := time.Now()
		for i := 0; i < n; i++ {
			var a common.Address
			a[0], a[1], a[2] = byte(i), byte(i>>8), byte(i>>16)
			shard := r.pickShard(a)
			s := track(r, &sender{addr: a, shard: shard, queued: map[uint64]*entry{}, posted: map[uint64]*entry{}})
			s.queued[0] = funderEntry(a, 0)
			r.load[shard]++
		}
		return time.Since(start)
	}
	first := pick(2_000)
	later := pick(20_000)
	perFirst := float64(first) / 2_000
	perLater := float64(later) / 20_000
	// Linear in the sender count would make each of the later ones ~10x dearer.
	if perLater > 4*perFirst {
		t.Fatalf("assigning a shard costs %.0f ns with 2k senders and %.0f ns with 22k: it still scales with the sender count",
			perFirst, perLater)
	}
	// And the work is spread: every account got some.
	for shard, n := range r.load {
		if n == 0 {
			t.Fatalf("account %d was left idle: %v", shard, r.load)
		}
	}
}
