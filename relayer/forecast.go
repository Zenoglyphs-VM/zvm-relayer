package relayer

import (
	"context"
	"sync"
	"time"

	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
)

// The forecast-driven footprint cap (DECISIONS.md, *Still open*: "the
// footprint cap is two different things, and only one is consensus").
//
// How much of NoM a rollup's relayers use is **relayer policy**, not
// consensus: derivation cannot refuse a NoM block that has already been
// included — ignoring it would only waste the sender's plasma — so the cap
// lives here, where it is cheap to change.
//
// It has to be a forecast, because a momentum's contents are unknown until it
// exists. So the relayer watches what everyone *else* has been putting on NoM
// over the last few momentums and takes what is left, between a floor and a
// ceiling:
//
//	share = clamp(ceiling − others, floor, ceiling)
//
// Mainnet today averages 0.09 account blocks per momentum and is 96 % empty,
// so the share sits at the ceiling almost always; the floor is for a busy
// future. It matters more than it looks on a v1 network, where a full momentum
// admits accounts in effectively random order (D13): a relayer flooding NoM
// would not get priority over other users, it would crowd them out at random.
//
// The two rulesets are measured differently, because they are limited
// differently:
//
//   - **v1** caps a momentum at 100 account blocks, whatever they contain, so
//     the resource is a block count.
//   - **v2** caps it at maxBasePlasmaInMomentum, so the resource is plasma,
//     and the fusion price says how contested it already is.

const (
	// FootprintWindow is how many momentums the moving average covers. Three
	// hundred seconds of history: long enough not to chase one busy momentum,
	// short enough to notice a sustained one.
	FootprintWindow = 30
	// DefaultFootprintFloorBP and DefaultFootprintCeilingBP bound the share.
	DefaultFootprintFloorBP   = 2_500
	DefaultFootprintCeilingBP = 9_000
	// DefaultMinTipWei is the smallest priority fee a relayer carries: 50 gwei
	// per gas, half what senders in this tree offer (rollup.DefaultTipWei).
	//
	// The margin is deliberate. An effective tip is `min(tipCap, feeCap −
	// baseFee)`, so it *shrinks* as the base fee rises underneath a signed
	// transaction; a floor equal to the default would reject a transaction that
	// was perfectly well priced when it was signed, the moment the chain got
	// busy — which is exactly when it matters that it lands.
	DefaultMinTipWei = 50_000_000_000
	// v2's per-momentum base-plasma budget, go-zenon's default for
	// maxBasePlasmaInMomentum. Settable on chain with SetVariables, so this is
	// a starting point rather than a constant of the protocol.
	maxBasePlasmaInMomentum = 4_200_000
)

// relayedCeiling is the most gas one block will admit from the batch lane:
// the elastic rule's ceiling once the gas fork is scheduled (D10), and the
// whole block before it. A relayer that packs more than this into one batch is
// posting transactions the chain is certain to drop.
func relayedCeiling(cfg *rollup.Config) uint64 {
	if cfg == nil {
		return 0
	}
	if cfg.Forks.GasScheduled() && cfg.NativeFloorBP > 0 {
		return cfg.GasLimit - cfg.NativeFloor()
	}
	return cfg.GasLimit
}

// Footprint is the cap and what it is currently reading.
type Footprint struct {
	FloorBP   uint64
	CeilingBP uint64

	mu       sync.Mutex
	momentum uint64
	share    uint64 // basis points of NoM's capacity this relayer allows itself
	others   uint64 // what everyone else used, in the same units
	budget   int64  // what is left of this momentum's share, in unit()
	perMom   int64  // what the share allows per momentum, in unit()
	blocks   int    // full blocks that is worth, for /status
	v2       bool

	// gasLeft is the L2 gas this relayer may still offer the momentum it is
	// posting into, shared across every sender account. Without it each
	// account bounds only its *own* batch, and eight accounts posting
	// concurrently offer eight times what one block can admit: measured on
	// momentum 1714, 268,611,000 gas offered into a block admitting
	// 225,000,000, and the surplus 2,077 transactions were dropped "block
	// full" and re-posted in a later momentum — their bytes bought and paid
	// for on NoM twice.
	gasLeft   uint64
	gasPerMom uint64
}

// The budget is denominated in the resource NoM actually counts, because the
// two rulesets count different things (see the note above): blocks on v1,
// base plasma on v2. Converting a plasma share into a block count means
// costing every block as a full 16 KB one, and a relayer whose batches are
// 2 KB would then take a thirtieth of the share it is entitled to — which
// would show up in a v1-vs-v2 benchmark as dynamic plasma being far more
// restrictive than it is, when it was this policy being timid.
func (f *Footprint) cost(dataLen int) int64 {
	if f.v2 {
		return int64(znn.BaseSendPlasma(dataLen))
	}
	return 1
}

func (f *Footprint) unit() string {
	if f.v2 {
		return "plasma"
	}
	return "blocks"
}

// rollTo starts a fresh budget when the momentum has moved on. Height zero
// means the caller does not know the frontier yet, which is the state a
// relayer starts in — it must not read as "the same momentum, already spent".
func (f *Footprint) rollTo(height uint64) {
	if height != f.momentum || f.momentum == 0 {
		f.momentum, f.budget, f.gasLeft = height, f.perMom, f.gasPerMom
	}
}

// SetGasPerMomentum names the L2 gas one block will admit from this lane, which
// is the same number every sender has to share.
func (f *Footprint) SetGasPerMomentum(gas uint64) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gasPerMom, f.gasLeft = gas, gas
}

// GasRoom is the gas still unclaimed in the momentum at height. Zero means this
// relayer has already offered the block everything it can take, and posting
// more would only buy NoM space for transactions the engine will refuse.
func (f *Footprint) GasRoom(height uint64) uint64 {
	if f == nil || f.gasPerMom == 0 {
		return ^uint64(0)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollTo(height)
	return f.gasLeft
}

// SpendGas charges gas this relayer has now offered the momentum at height.
func (f *Footprint) SpendGas(height, gas uint64) {
	if f == nil || f.gasPerMom == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollTo(height)
	if gas >= f.gasLeft {
		f.gasLeft = 0
		return
	}
	f.gasLeft -= gas
}

// NewFootprint builds the policy. Zero bounds take the defaults; a ceiling of
// 10,000 with a floor to match turns the cap off in all but name.
func NewFootprint(floorBP, ceilingBP uint64) *Footprint {
	if floorBP == 0 {
		floorBP = DefaultFootprintFloorBP
	}
	if ceilingBP == 0 {
		ceilingBP = DefaultFootprintCeilingBP
	}
	if floorBP > ceilingBP {
		floorBP = ceilingBP
	}
	blocks := znn.MaxAccountBlocksInMomentum
	// Start with the budget the ceiling allows rather than with zero. This cap
	// is policy, not consensus, so an un-warmed forecast must fail *open*: a
	// zero budget here stopped a fresh chain's relayer from posting anything
	// until its first observation, which on a young chain is minutes away.
	return &Footprint{FloorBP: floorBP, CeilingBP: ceilingBP, share: ceilingBP,
		blocks: blocks, perMom: int64(blocks), budget: int64(blocks)}
}

// Observe folds recent momentums into the forecast. inbox names this rollup's
// inbox so that the relayer's own traffic — and any other publisher's to the
// same inbox — is not counted as somebody else's load.
func (f *Footprint) Observe(ms []*znn.DetailedMomentum, inbox znn.Address) {
	if len(ms) == 0 {
		return
	}
	var (
		usedBlocks, usedPlasma uint64
		counted                uint64
		v2                     bool
		newest                 uint64
	)
	for _, dm := range ms {
		m := dm.Momentum
		if m == nil {
			continue
		}
		counted++
		if m.Height > newest {
			newest = m.Height
		}
		if m.Version >= 2 {
			v2 = true
		}
		for _, b := range dm.Blocks {
			if b == nil || b.BlockType != znn.BlockTypeUserSend || b.ToAddress == inbox {
				continue
			}
			usedBlocks++
			usedPlasma += znn.BaseSendPlasma(len(b.Data))
		}
	}
	if counted == 0 {
		return
	}

	var others uint64 // basis points of NoM's capacity that others took
	if v2 {
		others = usedPlasma * 10_000 / (counted * maxBasePlasmaInMomentum)
	} else {
		others = usedBlocks * 10_000 / (counted * znn.MaxAccountBlocksInMomentum)
	}
	share := f.CeilingBP
	if others < f.CeilingBP {
		share = f.CeilingBP - others
	} else {
		share = 0
	}
	share = min(max(share, f.FloorBP), f.CeilingBP)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.v2, f.others, f.share = v2, others, share
	// The share becomes a budget in whichever resource the ruleset counts.
	switch {
	case v2:
		f.perMom = int64(share * maxBasePlasmaInMomentum / 10_000)
		f.blocks = max(int(f.perMom/f.cost(znn.MaxDataLength)), 1)
	default:
		f.blocks = max(int(share*znn.MaxAccountBlocksInMomentum/10_000), 1)
		f.perMom = int64(f.blocks)
	}
	if newest != f.momentum {
		f.momentum, f.budget = newest, f.perMom
	}
}

// Take asks whether the momentum at height still has room for this relayer. A
// relayer that is refused waits for the next momentum rather than crowding
// other users out at random.
//
// It is asked before the block is built, so the size is not known yet: Take
// charges the smallest a block can be and Spend charges the rest once the
// block is out. The overshoot is bounded by one block per sender, which for a
// policy cap is cheaper than either guessing high or holding the lock across
// a publish.
func (f *Footprint) Take(height uint64) bool {
	if f == nil {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollTo(height)
	if f.budget <= 0 {
		return false
	}
	f.budget -= f.cost(0)
	return true
}

// Spend charges a block of dataLen bytes that has just been posted, on top of
// what Take already reserved.
func (f *Footprint) Spend(height uint64, dataLen int) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollTo(height)
	f.budget -= f.cost(dataLen) - f.cost(0)
}

// Snapshot is what /status reports about the cap.
type FootprintStatus struct {
	ShareBP        uint64   `json:"shareBp"`
	OthersBP       uint64   `json:"othersBp"`
	BlocksPerMom   int      `json:"blocksPerMomentum"`
	BudgetLeft     int      `json:"budgetLeft"`
	BudgetPerMom   int      `json:"budgetPerMomentum"`
	GasRoom        uint64   `json:"gasRoom"`
	GasPerMom      uint64   `json:"gasPerMomentum"`
	Unit           string   `json:"unit"`
	Momentum       uint64   `json:"momentum"`
	DynamicPlasma  bool     `json:"dynamicPlasma"`
	FloorCeilingBP []uint64 `json:"floorCeilingBp"`
}

func (f *Footprint) Status() *FootprintStatus {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return &FootprintStatus{
		GasRoom: f.gasLeft, GasPerMom: f.gasPerMom,
		ShareBP: f.share, OthersBP: f.others, BlocksPerMom: f.blocks,
		BudgetLeft: int(f.budget), BudgetPerMom: int(f.perMom), Unit: f.unit(),
		Momentum: f.momentum, DynamicPlasma: f.v2, FloorCeilingBP: []uint64{f.FloorBP, f.CeilingBP},
	}
}

// watchFootprint keeps the forecast fed from the L1's recent momentums.
func (r *Relayer) watchFootprint(ctx context.Context) {
	if r.cfg.Footprint == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		frontier, err := r.cfg.L1.FrontierMomentum(ctx)
		if err == nil && frontier.Height > 0 {
			// Whatever history there is, even on a chain minutes old: waiting
			// for a full window left a new chain's relayer with no forecast at
			// all, and the poster reads the height from here.
			r.l1Height.Store(frontier.Height)
			from, count := uint64(1), frontier.Height
			if frontier.Height > FootprintWindow {
				from, count = frontier.Height-FootprintWindow, FootprintWindow
			}
			ms, err := r.cfg.L1.DetailedMomentums(ctx, from, count)
			if err == nil {
				r.cfg.Footprint.Observe(ms, r.cfg.Rollup.Inbox)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
