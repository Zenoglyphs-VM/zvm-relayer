package relayer

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"

	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
)

// The fleet benchmark's defect 1: a transaction whose nonce is above the
// account's is requeued whenever its nonce is unused — which a gapped one
// always is — so it is posted again forever.
func TestGappedNonceStopsBeingReposted(t *testing.T) {
	const maxRequeues = 8
	for _, c := range []struct {
		name                        string
		txNonce, chainNonce, cursor uint64
		requeues                    int
		wantCursor                  uint64
		wantKeep                    bool
	}{
		{"the next nonce is retried from where the account is", 5, 5, 6, 1, 5, true},
		{"a gapped nonce is kept but the cursor stays at the account's",
			9, 5, 10, 1, 5, true},
		{"a nonce something else already used is dropped", 4, 5, 6, 1, 5, false},
		{"and one that has been tried too often is given up on", 9, 5, 10, maxRequeues + 1, 5, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cursor, keep := requeuePlan(c.txNonce, c.chainNonce, c.cursor, c.requeues, maxRequeues)
			if cursor != c.wantCursor || keep != c.wantKeep {
				t.Fatalf("cursor %d keep %v, want %d %v", cursor, keep, c.wantCursor, c.wantKeep)
			}
		})
	}

	// The property that matters: after the cursor is put back, a gapped entry
	// is not reachable, so ready() cannot pick it up again.
	s := &sender{queued: map[uint64]*entry{}, posted: map[uint64]*entry{}}
	s.next, _ = requeuePlan(9, 5, 10, 1, maxRequeues)
	s.queued[9] = &entry{}
	if s.queued[s.next] != nil {
		t.Fatal("a gapped transaction is still the next thing to post")
	}
	// Once the gap is filled it becomes reachable again, as it should.
	for n := uint64(5); n < 9; n++ {
		s.queued[n] = &entry{}
	}
	for n := s.next; ; n++ {
		if s.queued[n] == nil {
			t.Fatalf("the run stopped at %d, before the gapped transaction", n)
		}
		if n == 9 {
			break
		}
	}
}

// A relayer on a chain minutes old has observed nothing yet. The footprint cap
// is policy rather than consensus, so an un-warmed forecast must let it post:
// starting at zero budget stopped a fresh chain's relayer until its first
// observation — measured 2026-09-16, that was the whole first 30 momentums,
// during which every transaction sat queued and `zvm bench setup` timed out.
func TestFreshFootprintLetsARelayerPost(t *testing.T) {
	f := NewFootprint(0, 0)
	// Height zero: the poster does not know the frontier yet.
	if !f.Take(0) {
		t.Fatal("a forecast that has seen nothing refused the first block")
	}
	if !f.Take(0) {
		t.Fatal("the budget should cover more than one block at height zero")
	}
	// And once a height is known, each new momentum starts a fresh budget.
	if !f.Take(7) {
		t.Fatal("refused at a known height")
	}
	s := f.Status()
	if s.BudgetLeft != s.BlocksPerMom-1 || s.Momentum != 7 {
		t.Fatalf("after one take at momentum 7: %+v", s)
	}
	// A spent momentum still refuses, which is the cap doing its job.
	for i := 0; i < s.BlocksPerMom; i++ {
		f.Take(7)
	}
	if f.Take(7) {
		t.Fatal("the cap did not bind once the momentum's budget was spent")
	}
	if !f.Take(8) {
		t.Fatal("the next momentum did not refill")
	}
}

// The forecast has to work on a chain shorter than its window.
func TestObserveOnAYoungChain(t *testing.T) {
	f := NewFootprint(0, 0)
	inbox := rollup.Inbox
	other := znn.MustDevWallet().Key(1).Address
	var ms []*znn.DetailedMomentum
	for h := uint64(1); h <= 3; h++ {
		dm := &znn.DetailedMomentum{Momentum: &znn.Momentum{Height: h, Version: 1}}
		// One block of somebody else's, one of this rollup's own.
		dm.Blocks = append(dm.Blocks,
			&znn.AccountBlock{BlockType: znn.BlockTypeUserSend, Address: other, ToAddress: other},
			&znn.AccountBlock{BlockType: znn.BlockTypeUserSend, Address: other, ToAddress: inbox})
		ms = append(ms, dm)
	}
	f.Observe(ms, inbox)
	s := f.Status()
	if s.Momentum != 3 || s.BudgetLeft <= 0 {
		t.Fatalf("three momentums of history: %+v", s)
	}
	if s.OthersBP == 0 {
		t.Fatal("somebody else's block should count against the share")
	}
}

// On v2 the momentum's limit is base plasma, which a 2 KB block spends far
// less of than a 16 KB one. Budgeting in whole 16 KB blocks let a relayer
// posting small batches take a thirtieth of its share — measured 2026-09-16 on
// chain 71, where /status showed 3 blocks per momentum against chain 70's 89 —
// and would have shown up in a v1-vs-v2 benchmark as dynamic plasma.
func TestAV2BudgetIsSpentBySizeNotByBlock(t *testing.T) {
	inbox := rollup.Inbox
	other := znn.MustDevWallet().Key(1).Address
	v2 := momentums(t, 1, 5, 0, inbox, other)
	for _, dm := range v2 {
		dm.Momentum.Version = 2
	}
	f := NewFootprint(2_500, 9_000)
	f.Observe(v2, inbox)
	st := f.Status()
	if st.Unit != "plasma" || st.BudgetPerMom <= 0 {
		t.Fatalf("a v2 forecast should budget plasma: %+v", st)
	}
	if st.BlocksPerMom > 4 {
		t.Fatalf("a full 16 KB block costs most of a momentum, so few fit: %d", st.BlocksPerMom)
	}

	// Small blocks: many more than BlocksPerMom must fit, because that count
	// is the full-block equivalent rather than the limit.
	const small = 2048
	posted := 0
	for f.Take(100) {
		f.Spend(100, small)
		posted++
		if posted > 200 {
			t.Fatal("the budget never ran out")
		}
	}
	if posted <= st.BlocksPerMom {
		t.Fatalf("posted %d blocks of %d bytes, no better than %d full ones", posted, small, st.BlocksPerMom)
	}
	if want := st.BudgetPerMom / int(znn.BaseSendPlasma(small)); posted < want-1 || posted > want+1 {
		t.Fatalf("posted %d blocks of %d bytes, the share pays for about %d", posted, small, want)
	}

	// A full block still costs a full block: the cap has not been loosened,
	// only denominated correctly.
	f = NewFootprint(2_500, 9_000)
	f.Observe(v2, inbox)
	full := 0
	for f.Take(100) {
		f.Spend(100, znn.MaxDataLength)
		full++
	}
	// One more than the share pays for, at most: Take admits a block while any
	// budget is left, so the last one of a momentum may overshoot by less than
	// a block. A cap that is policy rather than consensus fails open.
	if full < st.BlocksPerMom || full > st.BlocksPerMom+1 {
		t.Fatalf("%d full blocks fit, want %d or one more", full, st.BlocksPerMom)
	}
}

func TestRelayedCeilingFollowsTheFork(t *testing.T) {
	cfg := &rollup.Config{GasLimit: 300_000_000}
	if got := relayedCeiling(cfg); got != 300_000_000 {
		t.Fatalf("before the fork a batch may fill the block: %d", got)
	}
	cfg.Forks = &rollup.Forks{Gas: 10}
	cfg.NativeFloorBP, cfg.NativeCeilBP = 2_500, 7_500
	if got := relayedCeiling(cfg); got != 225_000_000 {
		t.Fatalf("with the elastic share the batch lane stops at the ceiling: %d", got)
	}
}

// The forecast has to be read off what is already committed, and has to leave
// room for everyone else.
func TestFootprintLeavesRoomForOtherUsers(t *testing.T) {
	inbox := rollup.Inbox
	other := znn.MustDevWallet().Key(1).Address

	quiet := momentums(t, 10, 100, 0, inbox, other)
	f := NewFootprint(2_500, 9_000)
	f.Observe(quiet, inbox)
	if st := f.Status(); st.ShareBP != 9_000 {
		t.Fatalf("an empty NoM should give the ceiling, got %d bp", st.ShareBP)
	}
	if !f.Take(109) {
		t.Fatal("the first block of a momentum must be allowed")
	}

	// Half of NoM's v1 block budget used by other people.
	busy := momentums(t, 10, 100, 50, inbox, other)
	f = NewFootprint(2_500, 9_000)
	f.Observe(busy, inbox)
	st := f.Status()
	if st.OthersBP != 5_000 {
		t.Fatalf("others took %d bp, want 5000", st.OthersBP)
	}
	if st.ShareBP != 4_000 {
		t.Fatalf("this relayer should take what is left: %d bp", st.ShareBP)
	}
	if st.BlocksPerMom != 40 {
		t.Fatalf("40 of a hundred blocks, got %d", st.BlocksPerMom)
	}

	// Saturated: the floor still guarantees something rather than nothing.
	full := momentums(t, 10, 100, 100, inbox, other)
	f = NewFootprint(2_500, 9_000)
	f.Observe(full, inbox)
	if st := f.Status(); st.ShareBP != 2_500 {
		t.Fatalf("the floor should hold at %d bp, got %d", 2_500, st.ShareBP)
	}

	// And the budget is spent, not unlimited.
	f = NewFootprint(2_500, 2_500)
	f.Observe(full, inbox)
	allowed := f.Status().BlocksPerMom
	for i := 0; i < allowed; i++ {
		if !f.Take(109) {
			t.Fatalf("refused block %d of %d", i, allowed)
		}
	}
	if f.Take(109) {
		t.Fatal("the budget should be spent")
	}
	if !f.Take(110) {
		t.Fatal("a new momentum refills it")
	}
}

// momentums builds `count` momentums, each carrying `others` sends from
// somebody else and one to the inbox, which must not count against the cap.
func momentums(t *testing.T, from, count uint64, others int, inbox, other znn.Address) []*znn.DetailedMomentum {
	t.Helper()
	var out []*znn.DetailedMomentum
	for h := from; h < from+count; h++ {
		dm := &znn.DetailedMomentum{Momentum: &znn.Momentum{Version: 1, Height: h}}
		add := func(to znn.Address) {
			b := &znn.AccountBlock{BlockType: znn.BlockTypeUserSend, Address: other, ToAddress: to}
			dm.Blocks = append(dm.Blocks, b)
		}
		for i := 0; i < others; i++ {
			add(other)
		}
		add(inbox)
		out = append(out, dm)
	}
	return out
}

// The base fee lands in a vault with no code, so it can never be paid to
// anyone: the tip is the only part of a relayed transaction that reaches the
// account that bought the plasma. A relayer is a mempool and may price that.
func TestAMinimumTipIsRelayerPolicy(t *testing.T) {
	baseFee := big.NewInt(150_000_000_000)
	min := big.NewInt(100_000_000_000)
	for _, c := range []struct {
		name   string
		feeCap *big.Int
		tipCap *big.Int
		want   bool
	}{
		{"a tip at the floor is carried", big.NewInt(250_000_000_000), min, true},
		{"above it too", big.NewInt(400_000_000_000), big.NewInt(200_000_000_000), true},
		{"a thousandth of a gwei is not", big.NewInt(200_000_000_000), big.NewInt(1_000_000), false},
		{"nor is nothing at all", baseFee, big.NewInt(0), false},
		// The effective tip is capped by what is left under the fee cap, so a
		// generous tipCap under a tight feeCap still fails — which is the case
		// a naive check on GasTipCap alone would wave through.
		{"a tip the fee cap cannot pay for is not", big.NewInt(150_000_000_001), big.NewInt(500_000_000_000), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			tx := types.NewTx(&types.DynamicFeeTx{GasFeeCap: c.feeCap, GasTipCap: c.tipCap})
			got := tx.EffectiveGasTipValue(baseFee).Cmp(min) >= 0
			if got != c.want {
				t.Fatalf("effective tip %s against a floor of %s: carried=%v, want %v",
					tx.EffectiveGasTipValue(baseFee), min, got, c.want)
			}
		})
	}
}

// Every sender shares one block's gas. `MaxBatchGas` bounds a single batch, so
// eight accounts posting into the same momentum each honoured it and together
// offered eight times what a block admits — measured on chain 70's momentum
// 1714: 268,611,000 gas offered into a block taking 225,000,000, and the
// surplus 2,077 transactions were refused "block full" and re-posted later,
// their bytes bought on NoM twice.
func TestTheLaneSharesOneBlocksGas(t *testing.T) {
	const ceiling = 225_000_000
	f := NewFootprint(0, 0)
	f.SetGasPerMomentum(ceiling)

	if room := f.GasRoom(10); room != ceiling {
		t.Fatalf("a fresh momentum offers the whole block: %d", room)
	}
	// Eight senders, each taking what is left rather than a private budget.
	offered := uint64(0)
	for shard := 0; shard < 8; shard++ {
		room := f.GasRoom(10)
		take := min(room, uint64(100_000_000))
		f.SpendGas(10, take)
		offered += take
	}
	if offered > ceiling {
		t.Fatalf("the lane offered %d gas into a block admitting %d", offered, ceiling)
	}
	if room := f.GasRoom(10); room != 0 {
		t.Fatalf("the block should be spoken for, %d left", room)
	}
	// The next momentum starts again, or the lane would stop after one block.
	if room := f.GasRoom(11); room != ceiling {
		t.Fatalf("a new momentum refills the gas: %d", room)
	}
	// Unset means unbounded, which is what a relayer with no ceiling configured
	// had before this existed.
	if room := NewFootprint(0, 0).GasRoom(10); room != ^uint64(0) {
		t.Fatalf("an unconfigured budget must not bound anything: %d", room)
	}
}

// The budget has to be seeded from the resolved ceiling, not from whatever the
// caller had before New filled in the defaults — seeding it early passed zero,
// which silently disabled the whole thing.
func TestNewSeedsTheLaneGasBudget(t *testing.T) {
	cfg := Config{
		Rollup:    &rollup.Config{L2ChainID: 7340470, GasLimit: 300_000_000, Forks: &rollup.Forks{Gas: 1}, NativeFloorBP: 2_500, NativeCeilBP: 7_500},
		Footprint: NewFootprint(0, 0),
	}
	New(cfg)
	if room := cfg.Footprint.GasRoom(1); room != 225_000_000 {
		t.Fatalf("the lane's budget should be the block's relayed ceiling, got %d", room)
	}
}
