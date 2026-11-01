package relayer

// The fixes for chain 69 on 2026-09-22, when 6,835 relayed transactions were
// dropped in derivation: 3,464 were copies of transactions that had already
// landed, 30 were sends the bench funder could not pay for, and all eight
// accounts spent the morning retrying publishes they had no plasma for.

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/Zenoglyphs-VM/zvm-core/derive"
	"github.com/Zenoglyphs-VM/zvm-core/devnet"
	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
)

const testL2Chain = 7340469

var ether = big.NewInt(1e18)

func zvm(n int64) *big.Int { return new(big.Int).Mul(big.NewInt(n), ether) }

// fakeZNN serves go-zenon JSON-RPC from answer. A nil answer is a method the
// test did not expect; json.RawMessage("null") is an explicit null.
func fakeZNN(t *testing.T, answer func(method string) any) *znn.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var in struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		json.NewDecoder(req.Body).Decode(&in)
		result := answer(in.Method)
		if result == nil {
			t.Errorf("unexpected L1 RPC %s", in.Method)
		}
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": in.ID, "result": result})
	}))
	t.Cleanup(srv.Close)
	return znn.NewClient(srv.URL)
}

// submitting gives testRelayer what submit reads: the chain's rules and a
// signer for them.
func submitting(r *Relayer) *Relayer {
	r.cfg.Rollup = &rollup.Config{L2ChainID: testL2Chain, Inbox: rollup.Inbox, L1StartHeight: 1,
		MaxTxGas: 30_000_000, MaxAssembledBytes: 1 << 20, MaxBatchBytes: 1 << 22}
	r.signer = types.LatestSignerForChainID(r.cfg.Rollup.L2ChainIDBig())
	return r
}

// send signs a transfer of value from key at nonce, with a tip of tip wei.
func send(t *testing.T, key *ecdsa.PrivateKey, nonce uint64, value *big.Int, tip int64) []byte {
	t.Helper()
	to := common.HexToAddress("0xaefdaac1f2aec761947f7e3ddaae683db7899e92")
	tx, err := types.SignNewTx(key, types.LatestSignerForChainID(big.NewInt(testL2Chain)), &types.DynamicFeeTx{
		ChainID: big.NewInt(testL2Chain), Nonce: nonce, GasTipCap: big.NewInt(tip), GasFeeCap: big.NewInt(tip + 1),
		Gas: 21_000, To: &to, Value: value,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// The bench funder's case: 117 ZNN, and sends of 32 each. The first three fit
// and the fourth would overdraw. It used to be accepted — every one of them
// was, checked against the whole balance — and then never landed.
func TestAnOverdraftIsRefusedAtTheDoor(t *testing.T) {
	var nonce atomic.Uint64
	nonce.Store(176)
	balance := zvm(117)
	r := submitting(testRelayer(fakeChain(t, &nonce, balance)))
	key, _ := crypto.GenerateKey()
	ctx := context.Background()

	for n := uint64(176); n <= 178; n++ {
		if _, err := r.Submit(ctx, send(t, key, n, zvm(32), 1)); err != nil {
			t.Fatalf("nonce %d, affordable: %v", n, err)
		}
	}
	_, err := r.Submit(ctx, send(t, key, 179, zvm(32), 1))
	var rej *RejectError
	if !errors.As(err, &rej) || !strings.Contains(rej.Reason, "insufficient funds") {
		t.Fatalf("a fourth 32-ZNN send against 117 ZNN was not refused as an overdraft: %v", err)
	}

	// A replacement is paid instead of the one it replaces, not as well.
	if _, err := r.Submit(ctx, send(t, key, 178, zvm(32), 5)); err != nil {
		t.Fatalf("replacing nonce 178 at a higher fee: %v", err)
	}

	// Once the first three have landed, the balance is what they left and
	// they are owed nothing more, even before the relayer has settled them.
	nonce.Store(179)
	balance.Set(zvm(21))
	r.head = headCache{}
	if _, err := r.Submit(ctx, send(t, key, 179, zvm(20), 1)); err != nil {
		t.Fatalf("after three landed, 20 ZNN against 21 was refused: %v", err)
	}
}

// A transaction the engine left out because its sender could not pay is
// dropped at once, and the nonces behind it are left where the poster cannot
// reach them. Each of the funder's thirty was posted eighteen times.
func TestAnUnaffordableHeadIsDroppedNotReposted(t *testing.T) {
	var nonce atomic.Uint64
	nonce.Store(179)
	r := testRelayer(fakeChain(t, &nonce, zvm(10)))
	from := common.HexToAddress("0x6b6656f76c55f2df38df9ad285a49fdeab725e41")
	s := track(r, &sender{addr: from, next: 179, queued: map[uint64]*entry{}, posted: map[uint64]*entry{}})
	for n := uint64(179); n <= 181; n++ {
		to := common.Address{}
		e := &entry{tx: types.NewTx(&types.DynamicFeeTx{Nonce: n, Gas: 21_000, GasFeeCap: big.NewInt(1), To: &to, Value: zvm(32)}),
			raw: make([]byte, 64), from: from, received: time.Now(), queuedAt: time.Now()}
		s.queued[n] = e
		r.remember(e)
		r.load[0]++
	}
	head := s.queued[179]
	b := post(r, s, 179, 180, 181)
	r.batches = []*batch{b}

	r.settle(context.Background(), b, map[common.Hash]bool{}, false)

	if _, ok := r.byHash[head.tx.Hash()]; ok || s.queued[179] != nil {
		t.Fatal("the head its sender cannot pay for was queued again")
	}
	if got := r.stats.Discarded.Load(); got != 1 {
		t.Fatalf("discarded %d, want the head alone", got)
	}
	if s.next != 179 {
		t.Fatalf("cursor %d, want the account's nonce 179", s.next)
	}
	if ready, _ := r.ready(0, laneGas); len(ready) != 0 {
		t.Fatalf("the poster can still reach nonces %v behind a head that was dropped", nonces(ready))
	}
}

// A block the L1 node still holds is waited for, however late. Requeuing it
// was what put a second copy of every transaction on NoM.
func TestAPendingBlockIsWaitedForNotReposted(t *testing.T) {
	var nonce atomic.Uint64
	nonce.Store(10)
	pending := znn.AccountBlock{Hash: znn.Hash{1}, Height: 5}
	r := testRelayer(fakeChain(t, &nonce))
	r.cfg.Rollup = &rollup.Config{L1StartHeight: 1}
	r.cfg.DropAfter = 2 * time.Minute
	r.cfg.L1 = fakeZNN(t, func(method string) any {
		if method == "ledger.getAccountBlockByHash" {
			return pending
		}
		return nil
	})
	s := track(r, &sender{addr: common.Address{9}, next: 10, queued: map[uint64]*entry{}, posted: map[uint64]*entry{}})
	b := post(r, s, 10, 11)
	b.block, b.height, b.published = pending.Hash, pending.Height, time.Now().Add(-3*time.Minute)
	r.batches, r.inFlight[0] = []*batch{b}, 1

	if err := r.track(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.batches) != 1 || len(s.posted) != 2 || r.stats.Requeued.Load() != 0 {
		t.Fatalf("a block still on the node was requeued after DropAfter: %d batches, %d posted, %d requeued",
			len(r.batches), len(s.posted), r.stats.Requeued.Load())
	}
	if !b.overdue {
		t.Fatal("the overdue block was not marked, so its wait is logged every tick")
	}
}

// A block the node no longer has, and no re-bid replaced, is gone: that is
// the one case where its transactions go back in the queue.
func TestAVanishedBlockIsRequeued(t *testing.T) {
	var nonce atomic.Uint64
	nonce.Store(10)
	r := testRelayer(fakeChain(t, &nonce))
	r.cfg.Rollup = &rollup.Config{L1StartHeight: 1}
	r.cfg.DropAfter = 2 * time.Minute
	r.cfg.L1 = fakeZNN(t, func(method string) any {
		if method == "ledger.getAccountBlockByHash" {
			return json.RawMessage("null")
		}
		return nil
	})
	s := track(r, &sender{addr: common.Address{9}, next: 10, queued: map[uint64]*entry{}, posted: map[uint64]*entry{}})
	b := post(r, s, 10, 11)
	b.block, b.published = znn.Hash{2}, time.Now().Add(-3*time.Minute)
	r.batches, r.inFlight[0], r.load[0] = []*batch{b}, 1, 2

	if err := r.track(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.batches) != 0 || len(s.queued) != 2 {
		t.Fatalf("a vanished block's transactions were not requeued: %d batches, %d queued", len(r.batches), len(s.queued))
	}
}

// With no plasma left for a block worth posting, blockRoom says so, and the
// poster waits instead of being refused every two seconds.
func TestBlockRoomIsZeroWhenThePlasmaIsGone(t *testing.T) {
	var current atomic.Uint64
	l1 := fakeZNN(t, func(method string) any {
		switch method {
		case "embedded.plasma.get":
			return znn.PlasmaInfo{CurrentPlasma: current.Load(), MaxPlasma: 10_500_000}
		case "ledger.getFrontierMomentum":
			return znn.Momentum{Height: 10, Version: 1}
		}
		return nil
	})
	r := testRelayer(nil)
	r.cfg.L1, r.cfg.MaxInFlight = l1, 8
	r.cfg.Accounts = []*znn.Account{znn.NewAccount(devnet.Key(202), l1, 69)}
	r.room, r.capacity = []int{0}, []uint64{0}

	for _, c := range []struct {
		name    string
		current uint64
		want    int
	}{
		// 21,000 + 68 × 1,024 = 90,632 buys the smallest block worth posting.
		{"not enough for one small block", 50_000, 0},
		// Under the per-slot share, but the account can still pay for one.
		{"enough for one small block", 100_000, minBlockData},
		{"a full account", 10_500_000, batchRoom},
	} {
		current.Store(c.current)
		if got := r.blockRoom(context.Background(), 0); got != c.want {
			t.Errorf("%s: room %d, want %d", c.name, got, c.want)
		}
	}
	if r.capacity[0] != 10_500_000 {
		t.Fatalf("capacity %d, want the account's max plasma", r.capacity[0])
	}
}

// A starved shard waits for the next momentum, and no longer than that.
func TestAStarvedShardWaitsOneMomentum(t *testing.T) {
	r := testRelayer(nil)
	r.starved = make([]starve, 1)
	r.l1Height.Store(100)
	r.starve(0)
	if !r.waitingForPlasma(0) {
		t.Fatal("a shard just refused for plasma is not waiting")
	}
	r.l1Height.Store(101)
	if r.waitingForPlasma(0) {
		t.Fatal("still waiting after a new momentum")
	}
}

// Load is weighed against plasma: a shard with ten times the plasma takes a
// sender before one with a quarter of its load.
func TestPickShardWeighsLoadByPlasma(t *testing.T) {
	r := testRelayer(nil)
	r.cfg.Accounts = make([]*znn.Account, 2)
	r.inFlight, r.load = []int{0, 0}, []int{5, 20}
	r.capacity = []uint64{0, 0}
	if got := r.pickShard(common.Address{1}); got != 0 {
		t.Fatalf("unweighted, picked %d, want the less loaded 0", got)
	}
	r.capacity = []uint64{10_500_000, 105_000_000}
	if got := r.pickShard(common.Address{1}); got != 1 {
		t.Fatalf("weighted, picked %d, want 1: 20 against 105 M is lighter than 5 against 10.5 M", got)
	}
}

// After a restart, a batch still on its way is adopted rather than forgotten,
// so a wallet resubmitting from it gets the hash back instead of a second post.
func TestARestartAdoptsWhatIsInFlight(t *testing.T) {
	key, _ := crypto.GenerateKey()
	raws := [][]byte{send(t, key, 4, zvm(1), 1), send(t, key, 5, zvm(1), 1), send(t, key, 6, zvm(1), 1)}
	derived := send(t, key, 9, zvm(1), 1)
	pendingData, err := derive.EncodeBatch(raws, derive.BatchZlib)
	if err != nil {
		t.Fatal(err)
	}
	derivedData, err := derive.EncodeBatch([][]byte{derived}, derive.BatchZlib)
	if err != nil {
		t.Fatal(err)
	}
	f := recoveryFixtureFor(t, 3, raws)
	f.blocks[1].Data = pendingData
	f.blocks[2].ToAddress = rollup.Inbox
	f.blocks[2].Data = derivedData
	f.blocks[2].ConfirmationDetail = &znn.ConfirmationDetail{MomentumHeight: 100}
	f.nonce = 5 // nonce 4 landed already
	f.rebuild()
	r := f.relayer()

	if err := r.recoverInFlight(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.batches) != 1 || r.inFlight[0] != 1 {
		t.Fatalf("adopted %d batches with %d in flight, want the one unconfirmed", len(r.batches), r.inFlight[0])
	}
	if got := len(r.batches[0].entries); got != 2 {
		t.Fatalf("adopted %d transactions, want nonces 5 and 6 (4 has landed)", got)
	}
	if r.capacity[0] != 105_000_000 {
		t.Fatalf("capacity %d, want the account's max plasma read at startup", r.capacity[0])
	}

	// The wallet resubmits nonce 5, which it has not seen land.
	if _, err := r.Submit(context.Background(), raws[1]); err != nil {
		t.Fatal(err)
	}
	if r.nQueued != 0 {
		t.Fatalf("the resubmission was queued to be posted a second time (%d queued)", r.nQueued)
	}
}
