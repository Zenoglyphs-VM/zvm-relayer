// Package relayer is the ZVM's batch lane: it takes signed EVM transactions
// from wallets that have no Zenon key (MetaMask, Rabby, anything that speaks
// eth_sendRawTransaction), packs them into NoM account blocks sent to the
// inbox, and pays the plasma for those blocks from fused QSR.
//
// A relayer orders nothing. It chooses which transactions go into its own
// blocks; NoM orders the blocks; derivation decides what executes.
package relayer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/Zenoglyphs-VM/zvm-core/derive"
	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
)

// batchRoom is the data budget of one NoM block after the message type and
// compression flag.
const batchRoom = znn.MaxDataLength - 2

type Config struct {
	// Recovery bounds complete startup history; exhaustion refuses admission.
	Recovery RecoveryLimits
	Limits   Limits
	Rollup   *rollup.Config
	L1       *znn.Client
	Eth      *ethclient.Client
	Accounts []*znn.Account

	// MaxWait is how long a ready transaction waits for company before its
	// batch is posted anyway.
	MaxWait time.Duration
	// MaxInFlight bounds unconfirmed blocks per account. A full 16 KB block
	// needs 1,135,112 plasma, so a 5,000-QSR account (10.5 M) holds nine.
	MaxInFlight int
	// DropAfter is how long a posted block may be missing from the L1 node —
	// neither there nor replaced by a re-bid — before its transactions are
	// queued again.
	//
	// A block the node still holds is waited for however long it takes, and
	// never re-posted. Requeuing it puts a second copy of every transaction on
	// NoM, and when the first copy confirms after all, the second is dropped
	// as "nonce too low": 3,464 of chain 69's 6,835 dropped transactions on
	// 2026-09-22, each paid for in plasma the relayer was already short of.
	DropAfter time.Duration

	// QueueTTL is how long a transaction may sit queued — waiting for a gap in
	// front of it to be filled, usually — before it is discarded. Without it a
	// gapped nonce is re-posted forever: it is never included, and its nonce is
	// always unused, so every requeue rule that asks "is this nonce free?"
	// answers yes. That is the livelock the fleet benchmark found, ~160 MB of
	// NoM ledger a day.
	QueueTTL time.Duration
	// MaxRequeues bounds how many times one transaction may be posted again
	// after a batch of its own failed to land it.
	MaxRequeues int
	// MaxBatchGas bounds the gas one batch may declare. A batch above what a
	// block can admit is guaranteed to have a tail dropped as "block full",
	// which orphans that sender's later nonces and makes overload cascade.
	MaxBatchGas uint64
	// MinTip is the smallest priority fee, in wei per gas, this relayer will
	// carry. Policy rather than consensus: derivation admits whatever is
	// published, so this only decides what this relayer agrees to publish.
	MinTip *big.Int

	// Footprint, when set, keeps this relayer inside a share of NoM's own
	// capacity. See forecast.go.
	Footprint *Footprint

	// RebidInterval is how often each account checks whether its oldest
	// unconfirmed block has fallen behind the network's plasma price, and
	// replaces it if so.
	//
	// This exists because the account can otherwise never rescue itself. A
	// block priced at yesterday's rate is not refused, it is *stranded*, and
	// every block behind it with it. `Account.publish` re-bids on its way to
	// sending something new — but a relayer whose queue is stuck behind that
	// block has nothing new to send, so `poster` never calls publish and the
	// re-bid never runs. Under dynamic plasma that deadlocks: measured on
	// chain 71, the relayer stopped posting entirely and a 400 tx/s run
	// delivered zero. Zero disables the check.
	RebidInterval time.Duration

	Log *slog.Logger
}

type state int

const (
	queued state = iota
	posted
)

type entry struct {
	tx       *types.Transaction
	raw      []byte
	from     common.Address
	received time.Time
	queuedAt time.Time
	requeues int
	state    state
	batch    *batch
}

type sender struct {
	rawBytes int64
	addr     common.Address
	shard    int
	// next is the lowest nonce not yet posted. Queued entries at or above it
	// are posted in order; a gap holds everything behind it.
	next    uint64
	queued  map[uint64]*entry
	posted  map[uint64]*entry
	lastUse time.Time
	// postedGas is what this sender's in-flight transactions declare. It is
	// held under one block's worth (see ready).
	postedGas uint64
}

// settleCursor puts the cursor where posting can actually resume: never below
// a nonce the chain has already used, and past whatever of this sender's is
// still in flight.
//
// Rewinding it onto a posted nonce left ready() — which walks queued entries
// only — with nothing to start from, and when those posted nonces were then
// included nothing moved it forward again. Measured 2026-09-16 on the fleet
// benchmark's funder: cursor 91, chain nonce 104, the requeued 104 unreachable,
// so it was swept, and every resubmission of it was swept the same way. The
// sender stayed wedged until it went idle long enough to be forgotten.
func (s *sender) settleCursor(floor uint64) {
	if s.next < floor {
		s.next = floor
	}
	for s.posted[s.next] != nil {
		s.next++
	}
}

// owed is what this sender's transactions still to land may cost: every one
// queued or in flight at or above floor, the account's nonce, except skip —
// the nonce a replacement is about to take over.
func (s *sender) owed(floor, skip uint64) (*big.Int, int) {
	sum, count := new(big.Int), 0
	for _, m := range []map[uint64]*entry{s.queued, s.posted} {
		for n, e := range m {
			if n < floor || n == skip {
				continue
			}
			sum.Add(sum, e.tx.Cost())
			count++
		}
	}
	return sum, count
}

// reachable reports the queued nonces ready() can get to: those in an unbroken
// run from the cursor, where the run may pass through nonces still in flight.
// Everything else queued is gapped — waiting on a nonce the relayer does not
// hold, which only the client can supply.
func (s *sender) reachable() map[uint64]bool {
	out := map[uint64]bool{}
	for n := s.next; ; n++ {
		if s.queued[n] != nil {
			out[n] = true
			continue
		}
		if s.posted[n] != nil {
			continue
		}
		return out
	}
}

type batch struct {
	shard int
	block znn.Hash
	// height is the account-block height `block` was published at. A re-bid
	// replaces the block at that height with a new hash, so the height is the
	// stable identity and the hash is not.
	height    uint64
	published time.Time
	entries   []*entry
	bytes     int
	confirmed bool
	momentum  uint64
	// overdue is set once the block has outlived DropAfter while the node
	// still holds it, so that the wait is logged once rather than every tick.
	overdue bool
}

// starve records that a shard could not pay for a block: the momentum it
// found out in and when. Plasma comes back as the account's blocks confirm,
// which is a momentum away, so asking again sooner only earns another refusal.
type starve struct {
	momentum uint64
	at       time.Time
}

type Relayer struct {
	admissionReady atomic.Bool
	runStarted     atomic.Bool
	recoveryPhase  atomic.Uint32
	cfg            Config
	signer         types.Signer

	mu      sync.Mutex
	senders map[common.Address]*sender
	// byShard is the senders assigned to each account, and load is how many of
	// their transactions are outstanding there. Both are maintained as
	// transactions arrive and settle, because the alternative — counting them
	// when asked — is what pickShard used to do: a scan of every sender, for
	// every shard, on the first submission of every sender, holding this lock.
	// At 100,000 wallets each submitting once, that decayed the lane from the
	// offered 200 tx/s to 35 (measured 2026-09-17 on chain 70).
	byShard  []map[common.Address]*sender
	load     []int
	byHash   map[common.Hash]*entry
	batches  []*batch
	wake     []chan struct{}
	inFlight []int
	room     []int // data bytes a shard's next block can afford, 0 = not yet known
	// capacity is each account's maximum plasma, 0 until read. pickShard
	// weighs load by it: the accounts are not funded alike, and on 2026-09-22
	// the one holding a tenth of the others' plasma was given an equal share
	// of the senders and logged the most refusals of the eight.
	capacity []uint64
	starved  []starve
	// adopted is closed once recoverInFlight has run. Submissions wait for it:
	// one that arrived first would be queued as new and then posted a second
	// time, which is the very thing adopting is for.
	adopted chan struct{}
	// Running totals, so /status costs nothing at a hundred thousand senders.
	nQueued       int
	nPosted       int
	retainedBytes int64
	admitOnce     sync.Once
	admission     chan struct{}

	head headCache

	// l1Height is the newest momentum this relayer has seen. A batch posted
	// now lands in the next momentum at the earliest, so that is the height
	// whose rules it has to be encoded for.
	l1Height atomic.Uint64

	stats Stats
}

// codec is the compression this relayer's next batch may use: the best one
// every node deriving the momentum it lands in can decode (D19). Before the
// wire fork that is zlib, which is what every chain has always decoded.
func (r *Relayer) codec() byte {
	h := r.l1Height.Load()
	if h == 0 {
		return derive.BatchZlib
	}
	return derive.BestCodec(r.cfg.Rollup.Forks, h+1)
}

// Stats is what /status reports.
type Stats struct {
	Received      atomic.Uint64
	Rejected      atomic.Uint64
	Posted        atomic.Uint64
	Included      atomic.Uint64
	Requeued      atomic.Uint64
	Discarded     atomic.Uint64
	Batches       atomic.Uint64
	BatchBytes    atomic.Uint64
	RawBytes      atomic.Uint64
	PublishErrors atomic.Uint64
	Rebids        atomic.Uint64
}

func New(cfg Config) *Relayer {
	if cfg.MaxWait == 0 {
		cfg.MaxWait = time.Second
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = 8
	}
	if cfg.DropAfter == 0 {
		cfg.DropAfter = 2 * time.Minute
	}
	if cfg.QueueTTL == 0 {
		cfg.QueueTTL = 5 * time.Minute
	}
	if cfg.MaxRequeues == 0 {
		cfg.MaxRequeues = 8
	}
	if cfg.MaxBatchGas == 0 {
		cfg.MaxBatchGas = relayedCeiling(cfg.Rollup)
	}
	// Seed the lane's shared gas budget here rather than at the call site: the
	// ceiling is only known once the default above has been applied, and a
	// caller that seeded it earlier would quietly pass zero and disable the
	// budget entirely.
	cfg.Footprint.SetGasPerMomentum(cfg.MaxBatchGas)
	if cfg.RebidInterval == 0 {
		// Roughly every other momentum: often enough that a wedge is cleared
		// within a momentum or two of forming, rare enough that it is one
		// cheap RPC per account.
		cfg.RebidInterval = 20 * time.Second
	}
	r := &Relayer{
		cfg:      cfg,
		signer:   types.LatestSignerForChainID(cfg.Rollup.L2ChainIDBig()),
		senders:  map[common.Address]*sender{},
		byHash:   map[common.Hash]*entry{},
		wake:     make([]chan struct{}, len(cfg.Accounts)),
		inFlight: make([]int, len(cfg.Accounts)),
		room:     make([]int, len(cfg.Accounts)),
		capacity: make([]uint64, len(cfg.Accounts)),
		starved:  make([]starve, len(cfg.Accounts)),
		adopted:  make(chan struct{}),
		byShard:  make([]map[common.Address]*sender, len(cfg.Accounts)),
		load:     make([]int, len(cfg.Accounts)),
	}
	for i := range r.wake {
		r.wake[i] = make(chan struct{}, 1)
		r.byShard[i] = map[common.Address]*sender{}
	}
	return r
}

// headCache holds what admission needs from the ZVM, all of it a property of
// a block rather than of a moment: the base fee, and each sender's balance and
// nonce. None can change without a new block, so one read per block is not an
// approximation of reading them per transaction — it is the same answer.
//
// Reading them per transaction is what the first benchmark did, and at 250
// submissions a second it opened two connections per transaction and ran the
// host out of ephemeral ports ("Only one usage of each socket address"), which
// looked from the outside like the relayer refusing load it had not refused.
type headCache struct {
	mu       sync.Mutex
	header   *types.Header
	accounts map[common.Address]*account
	fetched  time.Time
}

// account is a sender as the head block has it. The nonce is what tells a
// transaction still in flight from one the head already carries: only the
// first is owed out of the balance.
type account struct {
	balance *big.Int
	nonce   uint64
}

// headFor returns the current head and the sender's account in it, reusing
// whatever was already read for this block.
// No RPC is made while the lock is held. It used to be, and with one sender
// the cache hid it: the map is emptied every block, so at 100,000 distinct
// senders nearly every submission missed it and every miss serialized the
// whole lane behind one mutex for the length of an eth_getBalance.
// Measured 2026-09-17 on chain 70: 76 submissions a second at 100,000 wallets
// against 200 offered, while the same code did 206 at 1,000 wallets — where
// the cache covers everyone after the first pass.
func (r *Relayer) headFor(ctx context.Context, from common.Address) (*types.Header, *account, error) {
	limits, err := r.cfg.Limits.checked()
	if err != nil {
		return nil, nil, err
	}
	c := &r.head

	// A momentum is 10 s, so a head older than a second is worth re-reading;
	// one younger than that cannot have been superseded more than once.
	c.mu.Lock()
	header, fetched := c.header, c.fetched
	var acct *account
	if header != nil && time.Since(fetched) <= time.Second {
		acct = c.accounts[from]
	}
	c.mu.Unlock()
	if header != nil && acct != nil {
		return header, acct, nil
	}

	if header == nil || time.Since(fetched) > time.Second {
		h, err := r.cfg.Eth.HeaderByNumber(ctx, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("reading the ZVM head: %w", err)
		}
		c.mu.Lock()
		if c.header == nil || h.Number.Cmp(c.header.Number) != 0 {
			c.accounts = map[common.Address]*account{}
		}
		if c.header == nil || h.Number.Cmp(c.header.Number) >= 0 {
			c.header, c.fetched = h, time.Now()
		}
		header, acct = c.header, c.accounts[from]
		c.mu.Unlock()
		if acct != nil {
			return header, acct, nil
		}
	}

	// Read at the header this answer is for, so a head that moves underneath
	// cannot pair one block's balance with another's base fee. Both reads go
	// in one round trip: this is the path every new sender takes.
	var bal hexutil.Big
	var nonce hexutil.Uint64
	at := hexutil.EncodeBig(header.Number)
	elems := []rpc.BatchElem{
		{Method: "eth_getBalance", Args: []any{from, at}, Result: &bal},
		{Method: "eth_getTransactionCount", Args: []any{from, at}, Result: &nonce},
	}
	if err := r.cfg.Eth.Client().BatchCallContext(ctx, elems); err != nil {
		return nil, nil, err
	}
	for _, e := range elems {
		if e.Error != nil {
			return nil, nil, e.Error
		}
	}
	acct = &account{balance: (*big.Int)(&bal), nonce: uint64(nonce)}
	c.mu.Lock()
	if c.header != nil && c.header.Number.Cmp(header.Number) == 0 && (c.accounts[from] != nil || len(c.accounts) < limits.MaxSenders) {
		c.accounts[from] = acct
	}
	c.mu.Unlock()
	return header, acct, nil
}

// RejectError is a submission refused before it was queued.
type RejectError struct{ Reason string }

func (e *RejectError) Error() string { return e.Reason }

func reject(format string, args ...any) error {
	return &RejectError{Reason: fmt.Sprintf(format, args...)}
}

// Submit validates a raw transaction and queues it.
func (r *Relayer) Submit(ctx context.Context, raw []byte) (common.Hash, error) {
	r.stats.Received.Add(1)
	hash, err := r.submit(ctx, raw)
	if err != nil {
		r.stats.Rejected.Add(1)
	}
	return hash, err
}

func (r *Relayer) submit(ctx context.Context, raw []byte) (common.Hash, error) {
	limits, err := r.cfg.Limits.checked()
	if err != nil {
		return common.Hash{}, err
	}
	ctx, release, err := r.beginAdmission(ctx, limits)
	if err != nil {
		return common.Hash{}, err
	}
	defer release()
	if len(raw) > r.cfg.Rollup.MaxAssembledBytes-2 {
		return common.Hash{}, reject("transaction is %d bytes", len(raw))
	}
	raw = bytes.Clone(raw)
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(raw); err != nil {
		return common.Hash{}, reject("undecodable transaction: %v", err)
	}
	switch tx.Type() {
	case types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType, types.SetCodeTxType:
	default:
		return common.Hash{}, reject("transaction type %d is not accepted", tx.Type())
	}
	if tx.Protected() && tx.ChainId().Cmp(r.cfg.Rollup.L2ChainIDBig()) != 0 {
		return common.Hash{}, reject("wrong chain id %s (the ZVM is %d)", tx.ChainId(), r.cfg.Rollup.L2ChainID)
	}
	if tx.Gas() > r.cfg.Rollup.MaxTxGas {
		return common.Hash{}, reject("gas limit %d above the ZVM's per-transaction cap %d", tx.Gas(), r.cfg.Rollup.MaxTxGas)
	}
	if len(raw) > r.cfg.Rollup.MaxAssembledBytes-2 {
		return common.Hash{}, reject("transaction is %d bytes", len(raw))
	}
	from, err := types.Sender(r.signer, tx)
	if err != nil {
		return common.Hash{}, reject("invalid signature: %v", err)
	}
	if err := r.awaitRecovery(ctx); err != nil {
		return common.Hash{}, err
	}

	r.mu.Lock()
	if _, ok := r.byHash[tx.Hash()]; ok {
		r.mu.Unlock()
		return tx.Hash(), nil
	}
	r.mu.Unlock()

	head, acct, err := r.headFor(ctx, from)
	if err != nil {
		return common.Hash{}, err
	}
	if tx.GasFeeCap().Cmp(head.BaseFee) < 0 {
		return common.Hash{}, reject("max fee per gas %s is below the base fee %s", tx.GasFeeCap(), head.BaseFee)
	}
	// A relayer is this chain's mempool, and a mempool may ask to be paid. The
	// base fee is burned — it goes to a vault with no code — so the tip is the
	// only part of a relayed transaction that ever reaches the account buying
	// the plasma to post it. Without a floor here, a wallet tipping a
	// thousandth of a gwei gets its NoM block paid for by somebody else.
	if min := r.cfg.MinTip; min != nil && min.Sign() > 0 {
		if tip := tx.EffectiveGasTipValue(head.BaseFee); tip.Cmp(min) < 0 {
			return common.Hash{}, reject("priority fee %s is below this relayer's minimum of %s wei per gas", tip, min)
		}
	}
	if acct.balance.Cmp(tx.Cost()) < 0 {
		return common.Hash{}, reject("insufficient funds: balance %s, transaction costs up to %s", acct.balance, tx.Cost())
	}

	if tx.Nonce() > acct.nonce && tx.Nonce()-acct.nonce > limits.MaxNonceGap {
		return common.Hash{}, &CapacityError{"nonce gap", int64(min(tx.Nonce()-acct.nonce, ^uint64(0)>>1)), int64(min(limits.MaxNonceGap, ^uint64(0)>>1))}
	}
	if tx.Nonce() < acct.nonce {
		return common.Hash{}, reject("nonce too low: %d, chain is %d", tx.Nonce(), acct.nonce)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byHash[tx.Hash()] != nil {
		return tx.Hash(), nil
	}
	if err := ctx.Err(); err != nil {
		return common.Hash{}, err
	}
	s := r.senders[from]
	var previous *entry
	if s != nil {
		previous = s.queued[tx.Nonce()]
	}
	if err := r.checkCapacity(limits, from, len(raw), previous); err != nil {
		return common.Hash{}, err
	}
	if s == nil {
		s = &sender{addr: from, shard: r.pickShard(from), next: acct.nonce,
			queued: map[uint64]*entry{}, posted: map[uint64]*entry{}}
		r.senders[from] = s
		r.byShard[s.shard][from] = s
	} else if len(s.queued) == 0 && len(s.posted) == 0 {
		// Idle: nothing of this sender's is in flight anywhere, so it is safe
		// to move it to whichever account is least busy now.
		if shard := r.pickShard(from); shard != s.shard {
			delete(r.byShard[s.shard], from)
			s.shard = shard
			r.byShard[shard][from] = s
		}
	}
	n := tx.Nonce()
	if _, ok := s.posted[n]; ok {
		return common.Hash{}, reject("nonce %d is already in a posted batch", n)
	}
	if n < s.next && s.queued[n] == nil {
		return common.Hash{}, reject("nonce too low: %d, next is %d", n, s.next)
	}
	// A sender's transactions are paid for one after another, so each has to
	// fit in what the ones before it leave. Checked against the whole balance
	// alone, the bench funder queued thirty-three 32-ZNN sends against 117 ZNN
	// on 2026-09-22: the engine could run three, and the other thirty — with
	// 532 later nonces stuck behind them — were posted for a quarter of an
	// hour and never landed. geth's pool refuses the same case as an overdraft.
	if owed, count := s.owed(acct.nonce, n); owed.Sign() > 0 {
		if total := new(big.Int).Add(owed, tx.Cost()); total.Cmp(acct.balance) > 0 {
			return common.Hash{}, reject("insufficient funds: balance %s, and with the %d transactions already queued this one would cost up to %s",
				acct.balance, count, total)
		}
	}
	if old := s.queued[n]; old != nil {
		// Replacement: only for a better fee, as a mempool would.
		if tx.GasFeeCap().Cmp(old.tx.GasFeeCap()) <= 0 || tx.GasTipCap().Cmp(old.tx.GasTipCap()) <= 0 {
			return common.Hash{}, reject("replacement transaction underpriced")
		}
		r.forget(old.tx.Hash())
	}
	now := time.Now()
	if old := s.queued[n]; old != nil {
		r.nQueued--
	}
	e := &entry{tx: tx, raw: raw, from: from, received: now, queuedAt: now}
	if s.queued[n] == nil {
		r.load[s.shard]++
	}
	s.queued[n] = e
	r.nQueued++
	s.lastUse = now
	r.remember(e)
	r.poke(s.shard)
	return tx.Hash(), nil
}

// pickShard chooses the NoM account a sender's transactions go out on.
//
// **A sender stays on one account while it has anything in flight**, and that
// is not an oversight: NoM orders blocks by address within a momentum and by
// height within an account, so two of a sender's nonces posted from two
// different accounts can arrive in the wrong order, and the later one is then
// dropped as "nonce too high". One account per sender makes the order the
// relayer chose the order derivation sees.
//
// What it costs is the fleet benchmark's defect 5: a single heavy sender can
// only ever use its own account's in-flight window. What is fixed here is the
// other half of it — the account was picked by hashing the address, so an
// unlucky spread left accounts idle while one was saturated. Now the least
// loaded account is chosen, and an idle sender is re-placed on its next
// submission.
//
// Load is weighed against each account's plasma, since that is what it posts
// with and the accounts are not funded alike. Until every account's plasma has
// been read they count as equal, which is what this always did.
func (r *Relayer) pickShard(a common.Address) int {
	weighted := len(r.capacity) == len(r.cfg.Accounts)
	for _, c := range r.capacity {
		if c == 0 {
			weighted = false
		}
	}
	capOf := func(shard int) uint64 {
		if weighted {
			return r.capacity[shard]
		}
		return 1
	}
	best, bestLoad := 0, -1
	for shard := range r.cfg.Accounts {
		load := r.inFlight[shard]*1_000 + r.load[shard]
		// load/capacity against bestLoad/capacity(best), cross-multiplied.
		// Both sides stay far inside a uint64: a load is at most millions and
		// a capacity at most billions.
		if bestLoad < 0 || uint64(load)*capOf(best) < uint64(bestLoad)*capOf(shard) {
			best, bestLoad = shard, load
		}
	}
	// A tie goes to the hash, so an idle relayer still spreads senders around
	// instead of stacking them all on account 0.
	if bestLoad == 0 {
		h := fnv.New32a()
		h.Write(a[:])
		return int(h.Sum32() % uint32(len(r.cfg.Accounts)))
	}
	return best
}

func (r *Relayer) poke(shard int) {
	select {
	case r.wake[shard] <- struct{}{}:
	default:
	}
}

// Run starts one poster per account and the inclusion tracker.
func (r *Relayer) Run(ctx context.Context) error {
	if !r.runStarted.CompareAndSwap(false, true) {
		return errors.New("relayer Run may only start once")
	}
	if _, err := r.cfg.Limits.checked(); err != nil {
		r.recoveryPhase.Store(2)
		if r.adopted != nil {
			close(r.adopted)
		}
		return err
	}
	if err := r.recoverInFlight(ctx); err != nil {
		r.recoveryPhase.Store(2)
		if r.adopted != nil {
			close(r.adopted)
		}
		return fmt.Errorf("complete startup recovery failed: %w", err)
	}
	if err := ctx.Err(); err != nil {
		r.recoveryPhase.Store(2)
		if r.adopted != nil {
			close(r.adopted)
		}
		return err
	}
	r.admissionReady.Store(true)
	r.recoveryPhase.Store(1)
	if r.adopted != nil {
		close(r.adopted)
	}
	defer func() { r.admissionReady.Store(false); r.recoveryPhase.Store(3) }()

	var wg sync.WaitGroup
	for i := range r.cfg.Accounts {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			r.poster(ctx, shard)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.tracker(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.watchFootprint(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.rebidder(ctx)
	}()
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

// ready collects the next postable run of transactions for a shard, oldest
// senders first, within one block's budget, and reports whether posting now
// is worthwhile.
func (r *Relayer) ready(shard int, gasCap uint64) ([]*entry, bool) {
	var senders []*sender
	for _, s := range r.byShard[shard] {
		if s.queued[s.next] != nil {
			senders = append(senders, s)
		}
	}
	sort.Slice(senders, func(i, j int) bool {
		return senders[i].queued[senders[i].next].received.Before(senders[j].queued[senders[j].next].received)
	})
	var out []*entry
	raw := 0
	var gas uint64
	oldest := time.Now()
	for _, s := range senders {
		// One sender never has more than a block's worth in flight. More can
		// only land across several momentums, and NoM's content order is not
		// this sender's nonce order: a later nonce that arrives first fills the
		// lane, the head is refused "block full", and everything behind it is
		// refused "nonce too high" — measured on the fleet funder, whose 16.75 M
		// gas disperse calls had nonces 358–366 in flight with the chain at 91.
		flight := s.postedGas
		for n := s.next; ; n++ {
			e := s.queued[n]
			if e == nil {
				break
			}
			if flight > 0 && flight+e.tx.Gas() > r.cfg.MaxBatchGas {
				break
			}
			// Raw size is an upper bound on the compressed size; stop at a
			// generous multiple and let compression decide the cut.
			if raw+len(e.raw) > 3*batchRoom && len(out) > 0 {
				return out, true
			}
			// And stop at what a block can actually admit. A batch that
			// declares more gas than the block has is guaranteed to have a
			// tail dropped as "block full" — and dropping one of a sender's
			// transactions orphans every later nonce of its own that is
			// already in flight, which is how overload cascades.
			if gas+e.tx.Gas() > gasCap && len(out) > 0 {
				return out, true
			}
			out = append(out, e)
			raw += len(e.raw)
			gas += e.tx.Gas()
			flight += e.tx.Gas()
			if e.received.Before(oldest) {
				oldest = e.received
			}
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, raw >= batchRoom || time.Since(oldest) >= r.cfg.MaxWait
}

func (r *Relayer) poster(ctx context.Context, shard int) {
	acct := r.cfg.Accounts[shard]
	log := r.cfg.Log.With("shard", shard, "account", acct.Address())
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-r.wake[shard]:
		}
		for {
			if r.waitingForPlasma(shard) {
				break
			}
			r.mu.Lock()
			if r.inFlight[shard] >= r.cfg.MaxInFlight {
				r.mu.Unlock()
				break
			}
			// What this momentum will still admit from the lane, shared with every
			// other sender: offering more only buys NoM space for transactions the
			// engine is certain to refuse.
			gasCap := min(r.cfg.MaxBatchGas, r.cfg.Footprint.GasRoom(r.l1Height.Load()))
			candidates, now := r.ready(shard, gasCap)
			if !now {
				r.mu.Unlock()
				break
			}
			r.mu.Unlock()

			if gasCap == 0 {
				// The block is already fully spoken for. Wait for the next momentum
				// rather than pay for bytes the engine will refuse.
				break
			}
			// Asked before Take, so that a shard which cannot pay does not
			// spend the momentum's footprint on a block it will not post.
			room := r.blockRoom(ctx, shard)
			if room == 0 {
				r.starve(shard)
				log.Debug("waiting a momentum for plasma")
				break
			}
			if !r.cfg.Footprint.Take(r.l1Height.Load()) {
				// NoM has no room for this relayer in this momentum. Wait for
				// the next one rather than crowd other users out at random.
				break
			}
			entries, payloads, err := r.encode(candidates, room)
			if err != nil {
				log.Error("encoding batch", "err", err)
				break
			}
			// Charge the forecast what each block actually costs NoM, as it goes
			// out. On v2 that is plasma, where a small block costs a fraction of
			// the full one Take had to assume before the batch was built.
			published, err := r.publish(ctx, acct, payloads, func(n int) {
				r.cfg.Footprint.Spend(r.l1Height.Load(), n)
			})
			if err != nil {
				r.stats.PublishErrors.Add(1)
				log.Warn("publishing batch", "err", err, "txs", len(entries))
				acct.Reset()
				if strings.Contains(err.Error(), "not enough plasma") {
					// blockRoom read the plasma a moment ago and another block
					// took it since. It comes back when blocks confirm, not in
					// two seconds: retrying on a timer is what logged 45,000 of
					// these on chain 69 in one morning.
					r.starve(shard)
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
				break
			}
			var offered uint64
			for _, e := range entries {
				offered += e.tx.Gas()
			}
			r.cfg.Footprint.SpendGas(r.l1Height.Load(), offered)
			b := &batch{shard: shard, block: published.Hash, height: published.Height, published: time.Now(), entries: entries}
			for _, p := range payloads {
				b.bytes += len(p)
			}
			r.mu.Lock()
			for _, e := range entries {
				s := r.senders[e.from]
				delete(s.queued, e.tx.Nonce())
				r.nQueued--
				s.posted[e.tx.Nonce()] = e
				s.postedGas += e.tx.Gas()
				r.nPosted++
				if e.tx.Nonce() >= s.next {
					s.next = e.tx.Nonce() + 1
				}
				e.state, e.batch = posted, b
				r.stats.RawBytes.Add(uint64(len(e.raw)))
			}
			r.batches = append(r.batches, b)
			r.inFlight[shard]++
			r.mu.Unlock()
			r.stats.Batches.Add(1)
			r.stats.Posted.Add(uint64(len(entries)))
			r.stats.BatchBytes.Add(uint64(b.bytes))
			log.Debug("posted batch", "block", b.block, "height", b.height, "txs", len(entries), "bytes", b.bytes)
		}
	}
}

// starveFor bounds how long a shard waits for plasma when the momentum height
// is not advancing where it can see it. The devnets' measured cadence is about
// fifteen seconds.
const starveFor = 15 * time.Second

// starve marks a shard as unable to pay for a block until the next momentum.
func (r *Relayer) starve(shard int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if shard < len(r.starved) {
		r.starved[shard] = starve{momentum: r.l1Height.Load(), at: time.Now()}
	}
}

// waitingForPlasma reports whether a shard is still waiting out a starve: until
// a later momentum has been seen, or starveFor has passed.
func (r *Relayer) waitingForPlasma(shard int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if shard >= len(r.starved) || r.starved[shard].at.IsZero() {
		return false
	}
	st := r.starved[shard]
	if r.l1Height.Load() > st.momentum || time.Since(st.at) > starveFor {
		r.starved[shard] = starve{}
		return false
	}
	return true
}

// encode fits as many candidates as compress into one block. A single
// transaction too large for a block goes out as chunks and an assemble.
func (r *Relayer) encode(candidates []*entry, room int) ([]*entry, [][]byte, error) {
	n := len(candidates)
	for n > 0 {
		raws := make([][]byte, n)
		for i := 0; i < n; i++ {
			raws[i] = candidates[i].raw
		}
		data, err := derive.EncodeBatch(raws, r.codec())
		if err != nil {
			return nil, nil, err
		}
		if len(data) <= room {
			return candidates[:n], [][]byte{data}, nil
		}
		if n == 1 {
			return candidates[:1], derive.SplitChunksRoom(data, room), nil
		}
		// Shrink in proportion to the overshoot, and by at least one.
		next := n * room / len(data)
		if next >= n {
			next = n - 1
		}
		n = max(next, 1)
	}
	return nil, nil, errors.New("nothing to encode")
}

// publish sends a batch, or its chunks and assemble. It returns the block
// whose confirmation decides the batch's momentum: the last one.
//
// The whole block is returned, not just its hash, because a re-bid replaces a
// block's hash while keeping its height — so the height is what lets the
// tracker find the batch again afterwards (see track).
// publish sends one batch. A payload too large for a NoM block goes as chunks
// plus an assemble, so this may be several blocks; charge is called with each
// block's size as it goes out, because the footprint's budget is spent per
// block and the assemble is a block like any other.
func (r *Relayer) publish(ctx context.Context, acct *znn.Account, payloads [][]byte, charge func(int)) (*znn.AccountBlock, error) {
	inbox, zero := r.cfg.Rollup.Inbox, new(big.Int)
	send := func(p []byte) (*znn.AccountBlock, error) {
		b, err := acct.Send(ctx, inbox, znn.ZeroTokenStandard, zero, p)
		if err == nil && charge != nil {
			charge(len(p))
		}
		return b, err
	}
	if len(payloads) == 1 {
		return send(payloads[0])
	}
	var hashes []znn.Hash
	for _, p := range payloads {
		b, err := send(p)
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, b.Hash)
	}
	return send(derive.EncodeAssemble(hashes))
}

// rebidder replaces each account's oldest unconfirmed block when the network's
// plasma price has moved past what that block paid.
//
// Nothing else can do this once the relayer is stuck. `Account.publish`
// re-bids on its way to sending something new — but a queue held behind a
// stranded block has nothing new to send, so `poster` never reaches publish,
// and the account waits for a confirmation that cannot come. (It used to be
// worse: `DropAfter` requeued the batch, and every requeued transaction was
// posted a second time behind the block still holding the first.) Doing it on
// a timer breaks that chain at its first link.
//
// It is cheap when there is nothing to do: Rebid reads the frontier momentum
// and the account's unconfirmed blocks, and returns nil unless the oldest one
// is actually underpaying.
func (r *Relayer) rebidder(ctx context.Context) {
	if r.cfg.RebidInterval <= 0 {
		return
	}
	ticker := time.NewTicker(r.cfg.RebidInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for shard, acct := range r.cfg.Accounts {
			bid, err := acct.Rebid(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				r.cfg.Log.Debug("re-bidding", "shard", shard, "account", acct.Address(), "err", err)
				continue
			}
			if bid == nil {
				continue
			}
			r.stats.Rebids.Add(1)
			r.cfg.Log.Warn("re-bid a block that had fallen behind the plasma price",
				"shard", shard, "account", acct.Address(), "height", bid.Height, "block", bid.Hash)
			r.poke(shard)
		}
	}
}

// tracker follows posted batches to NoM confirmation and then to the L2
// block derived from that momentum. A transaction that is not in that block
// was dropped by derivation; if its nonce is still unused it is queued again.
func (r *Relayer) tracker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := r.track(ctx); err != nil {
			r.cfg.Log.Warn("tracking batches", "err", err)
		}
		r.sweep()
	}
}

func (r *Relayer) track(ctx context.Context) error {
	r.mu.Lock()
	pending := append([]*batch(nil), r.batches...)
	r.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	l2Head, err := r.cfg.Eth.BlockNumber(ctx)
	if err != nil {
		return err
	}
	// One block read per momentum, shared by every batch that landed in it.
	derived := map[uint64]map[common.Hash]bool{}
	for _, b := range pending {
		if !b.confirmed {
			blk, err := r.cfg.L1.AccountBlockByHash(ctx, b.block)
			if err != nil {
				return err
			}
			if blk == nil {
				// The hash is gone, which on this account means a re-bid
				// replaced the block at that height with one paying the
				// current price. It carries the same payloads, so the batch
				// has not changed — only its identity has. Adopt the new hash
				// rather than waiting out DropAfter on a block that no longer
				// exists, which is what used to turn one re-bid into a
				// requeue of everything behind it.
				if repl, rerr := r.replacementFor(ctx, b); rerr != nil {
					return rerr
				} else if repl != nil {
					r.mu.Lock()
					b.block = repl.Hash
					r.mu.Unlock()
					blk = repl
				}
			}
			switch {
			case blk != nil && blk.ConfirmationDetail != nil:
				r.mu.Lock()
				b.confirmed, b.momentum = true, blk.ConfirmationDetail.MomentumHeight
				r.inFlight[b.shard]--
				r.mu.Unlock()
				r.poke(b.shard)
			case blk == nil && time.Since(b.published) > r.cfg.DropAfter:
				r.cfg.Log.Warn("batch block is gone from the L1 node; requeueing", "block", b.block, "txs", len(b.entries))
				r.settle(ctx, b, nil, true)
				continue
			case blk != nil && !b.overdue && time.Since(b.published) > r.cfg.DropAfter:
				// Still there, so it can still confirm, and a copy posted now
				// would land its transactions twice over. The re-bidder
				// handles a block that is stuck on price.
				r.mu.Lock()
				b.overdue = true
				r.mu.Unlock()
				r.cfg.Log.Warn("batch still unconfirmed; waiting for it rather than posting it again",
					"block", b.block, "height", b.height, "txs", len(b.entries), "age", time.Since(b.published).Truncate(time.Second))
				continue
			default:
				continue
			}
		}
		if l2Head < r.cfg.Rollup.L2BlockForMomentum(b.momentum) {
			continue
		}
		included, err := r.includedIn(ctx, b, derived)
		if err != nil {
			return err
		}
		r.settle(ctx, b, included, false)
	}
	return nil
}

// includedIn reports which of a batch's transactions the block derived from
// its momentum actually carries.
//
// It reads that block once — for every batch that shared the momentum — rather
// than asking for a receipt per transaction, which is what this did. A
// receipt each meant thousands of calls per second at 100,000 wallets: the
// tracker fell minutes behind, every account stayed at MaxInFlight because
// confirmations went unnoticed, and the lane posted nine NoM blocks a momentum
// where the L1 was idle and would have taken sixty. Measured 2026-09-17 on
// chain 70: 63.7 tx/s against 200 offered, with both L1 nodes reporting
// nothing unconfirmed while the relayer believed 64 blocks were in flight.
//
// A transaction the block does not carry may still have been included
// elsewhere — the same one can sit in two batches after a requeue — so those
// are checked by receipt, in one batched call rather than one call each.
func (r *Relayer) includedIn(ctx context.Context, b *batch, derived map[uint64]map[common.Hash]bool) (map[common.Hash]bool, error) {
	number := r.cfg.Rollup.L2BlockForMomentum(b.momentum)
	carried, ok := derived[number]
	if !ok {
		blk, err := r.cfg.Eth.BlockByNumber(ctx, new(big.Int).SetUint64(number))
		if err != nil {
			return nil, err
		}
		carried = make(map[common.Hash]bool, blk.Transactions().Len())
		for _, tx := range blk.Transactions() {
			carried[tx.Hash()] = true
		}
		derived[number] = carried
	}
	included := make(map[common.Hash]bool, len(b.entries))
	var elsewhere []*entry
	for _, e := range b.entries {
		if carried[e.tx.Hash()] {
			included[e.tx.Hash()] = true
			continue
		}
		elsewhere = append(elsewhere, e)
	}
	for i := 0; i < len(elsewhere); i += 500 {
		chunk := elsewhere[i:min(i+500, len(elsewhere))]
		elems := make([]rpc.BatchElem, len(chunk))
		results := make([]*types.Receipt, len(chunk))
		for j, e := range chunk {
			elems[j] = rpc.BatchElem{Method: "eth_getTransactionReceipt", Args: []any{e.tx.Hash()}, Result: &results[j]}
		}
		if err := r.cfg.Eth.Client().BatchCallContext(ctx, elems); err != nil {
			return nil, err
		}
		for j, e := range chunk {
			if elems[j].Error == nil && results[j] != nil {
				included[e.tx.Hash()] = true
			}
		}
	}
	return included, nil
}

// replacementFor finds the block now standing at a batch's height, for a batch
// whose own hash the node no longer knows. That happens when the account
// re-bids: every unconfirmed block is re-signed at the current price and
// re-chained, so the heights are preserved and the hashes are not.
//
// It returns nil when the height holds nothing, which means the block really
// is gone rather than replaced, and DropAfter should have it.
func (r *Relayer) replacementFor(ctx context.Context, b *batch) (*znn.AccountBlock, error) {
	if b.height == 0 {
		return nil, nil
	}
	addr := r.cfg.Accounts[b.shard].Address()
	list, err := r.cfg.L1.AccountBlocksByHeight(ctx, addr, b.height, 1)
	if err != nil {
		return nil, err
	}
	if list == nil || len(list.List) == 0 {
		return nil, nil
	}
	repl := list.List[0]
	if repl == nil || repl.Height != b.height || repl.Hash == b.block {
		return nil, nil
	}
	r.cfg.Log.Debug("batch block was replaced by a re-bid", "shard", b.shard,
		"height", b.height, "was", b.block, "now", repl.Hash)
	return repl, nil
}

// settle decides what becomes of a posted batch's transactions once the
// momentum that carried it has been derived: included, queued again, or
// discarded.
//
// **The account's own nonce is what decides**, not the transaction's. A
// transaction whose nonce is above the account's next is *gapped*: the chain
// will not take it until whatever sits in front of it lands, and its nonce is
// unused the whole time. Requeuing it and rewinding the sender's cursor to it
// — which is what this did — posts it again every second, forever, for a
// transaction that can never be included. The relayer's cursor therefore goes
// back to the account's nonce and no further, which leaves a gapped entry in
// the queue but out of reach until the gap is filled, exactly as a fresh
// out-of-order submission would be.
func (r *Relayer) settle(ctx context.Context, b *batch, included map[common.Hash]bool, unconfirmed bool) {
	nonces := map[common.Address]uint64{}
	unaffordable := map[common.Hash]bool{}
	for _, e := range b.entries {
		if included[e.tx.Hash()] {
			continue
		}
		if _, ok := nonces[e.from]; !ok {
			n, err := r.cfg.Eth.NonceAt(ctx, e.from, nil)
			if err != nil {
				return // try again on the next tick
			}
			nonces[e.from] = n
		}
		if e.tx.Nonce() == nonces[e.from] {
			_, acct, err := r.headFor(ctx, e.from)
			if err != nil {
				return
			}
			unaffordable[e.tx.Hash()] = acct.balance.Cmp(e.tx.Cost()) < 0
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// The lowest nonce each sender may post from once this batch is settled:
	// the account's nonce where it was read, and past every included one
	// otherwise, since an included nonce is a used one.
	floors := map[common.Address]uint64{}
	raise := func(a common.Address, n uint64) {
		if n > floors[a] {
			floors[a] = n
		}
	}
	for _, e := range b.entries {
		s := r.senders[e.from]
		delete(s.posted, e.tx.Nonce())
		s.postedGas -= e.tx.Gas()
		r.nPosted--
		if included[e.tx.Hash()] {
			r.forget(e.tx.Hash())
			r.stats.Included.Add(1)
			r.load[s.shard]--
			raise(e.from, e.tx.Nonce()+1)
			continue
		}
		chainNonce := nonces[e.from]
		raise(e.from, chainNonce)
		if unaffordable[e.tx.Hash()] {
			// The engine leaves out a transaction its sender cannot pay for,
			// and posting it again changes nothing until the balance does. It
			// used to be posted until its retries ran out — the bench funder's
			// thirty were each posted eighteen times on 2026-09-22. The nonces
			// behind it are left gapped, and the sweep takes them.
			if s.next > chainNonce {
				s.next = chainNonce
			}
			r.load[s.shard]--
			r.discard(e)
			r.cfg.Log.Info("dropping a transaction its sender cannot pay for", "tx", e.tx.Hash(), "from", e.from,
				"nonce", e.tx.Nonce(), "cost", e.tx.Cost())
			continue
		}
		// Only the transaction at the account's nonce failed on its own. One
		// above it was refused for the gap in front of it, and counting that
		// against it spent the retries of every nonce behind a single "block
		// full" — so the head, when its turn came, had none left.
		if e.tx.Nonce() <= chainNonce {
			e.requeues++
		}
		cursor, keep := requeuePlan(e.tx.Nonce(), chainNonce, s.next, e.requeues, r.cfg.MaxRequeues)
		s.next = cursor
		if !keep {
			r.load[s.shard]--
			r.discard(e)
			r.cfg.Log.Debug("giving up on a transaction", "tx", e.tx.Hash(), "from", e.from,
				"nonce", e.tx.Nonce(), "accountNonce", chainNonce, "attempts", e.requeues)
			continue
		}
		// A fresh wait: QueueTTL is for how long an entry has been stuck behind a
		// gap, not how long ago the client first sent it.
		e.state, e.batch, e.queuedAt = queued, nil, time.Now()
		s.queued[e.tx.Nonce()] = e
		r.nQueued++
		r.stats.Requeued.Add(1)
	}
	for a, floor := range floors {
		r.senders[a].settleCursor(floor)
	}
	if unconfirmed {
		r.inFlight[b.shard]--
	}
	for i, x := range r.batches {
		if x == b {
			r.batches = append(r.batches[:i], r.batches[i+1:]...)
			break
		}
	}
	r.poke(b.shard)
}

// requeuePlan decides what happens to a posted transaction that was not
// included: where the sender's cursor goes, and whether the entry is worth
// keeping.
//
// The cursor is what makes the difference between retrying and livelocking.
// It is "the lowest nonce not yet posted", and ready() walks up from it while
// entries are contiguous — so putting it back at the *account's* nonce leaves
// a gapped entry reachable only once the gap is filled, while putting it back
// at the *transaction's* nonce re-posts something the chain cannot accept,
// every second, forever.
func requeuePlan(txNonce, chainNonce, cursor uint64, requeues, maxRequeues int) (uint64, bool) {
	if chainNonce < cursor {
		cursor = chainNonce
	}
	if txNonce < chainNonce {
		return cursor, false // its nonce has been used by something else
	}
	if requeues > maxRequeues {
		return cursor, false
	}
	return cursor, true
}

// discard forgets a transaction the relayer will never land. Callers hold the
// lock, and the entry must already be out of its sender's maps.
func (r *Relayer) discard(e *entry) {
	r.forget(e.tx.Hash())
	r.stats.Discarded.Add(1)
}

// sweep drops queued transactions that have waited too long — almost always a
// gapped nonce whose predecessor never arrived — and forgets senders with
// nothing left, so neither the queue nor the sender map grows without bound.
func (r *Relayer) sweep() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for addr, s := range r.senders {
		// Only a gapped entry is stale. One that is merely waiting its turn —
		// behind this sender's own earlier nonces, or for gas in the lane — is
		// going out, and dropping it would open the very gap that strands the
		// rest: the fleet funder's 289 calls need ~22 momentums at 13 a block,
		// longer than QueueTTL, so the tail was discarded and never arrived.
		live := s.reachable()
		for n, e := range s.queued {
			if live[n] || now.Sub(e.queuedAt) <= r.cfg.QueueTTL {
				continue
			}
			delete(s.queued, n)
			r.nQueued--
			r.load[s.shard]--
			r.discard(e)
			r.cfg.Log.Debug("discarding a stale transaction", "tx", e.tx.Hash(), "from", e.from,
				"nonce", n, "waited", now.Sub(e.queuedAt).Truncate(time.Second))
		}
		if len(s.queued) == 0 && len(s.posted) == 0 && now.Sub(s.lastUse) > r.cfg.QueueTTL {
			delete(r.senders, addr)
			delete(r.byShard[s.shard], addr)
		}
	}
}

// Snapshot is the relayer's status.
type Snapshot struct {
	RetainedRawBytes int64    `json:"retainedRawBytes"`
	Limits           Limits   `json:"limits"`
	Ready            bool     `json:"ready"`
	RecoveryStatus   string   `json:"recoveryStatus"`
	AdmissionMode    string   `json:"admissionMode"`
	AdmissionDurable bool     `json:"admissionDurable"`
	Accounts         []string `json:"accounts"`
	Queued           int      `json:"queued"`
	PostedPending    int      `json:"postedPending"`
	InFlight         []int    `json:"inFlightBlocks"`
	Senders          int      `json:"senders"`
	Received         uint64   `json:"received"`
	Rejected         uint64   `json:"rejected"`
	Posted           uint64   `json:"posted"`
	Included         uint64   `json:"included"`
	Requeued         uint64   `json:"requeued"`
	Discarded        uint64   `json:"discarded"`
	Batches          uint64   `json:"batches"`
	BatchBytes       uint64   `json:"batchBytes"`
	RawBytes         uint64   `json:"rawBytes"`
	PublishErrors    uint64   `json:"publishErrors"`
	// Rebids counts blocks replaced because they had fallen behind the plasma
	// price. On a chain without dynamic plasma it stays zero.
	Rebids uint64 `json:"rebids"`
	// SenderQueues is the per-sender queue, for the senders that have anything
	// outstanding, deepest first. Without it there is no way to see from
	// outside which account a stuck queue belongs to, or what nonce it is
	// waiting on — which is exactly what a nonce-gap stall looks like.
	SenderQueues []SenderStatus `json:"senderQueues,omitempty"`

	// Footprint is the share of NoM this relayer is allowing itself, when the
	// cap is on.
	Footprint *FootprintStatus `json:"footprint,omitempty"`
}

// NextNonce is the nonce this relayer would accept next from a sender: the
// chain's nonce, advanced past everything of that sender's it is already
// holding.
//
// A caller cannot work this out for itself. `eth_getTransactionCount` on
// `latest` counts only what has been derived, and `pending` is no better here —
// **this relayer is the mempool**, and a transaction queued in it was never in
// the execution client's pool at all. So a client that reads the chain nonce
// while the relayer still holds work for that sender picks a nonce that is
// already queued, and the resubmission is refused as an underpriced
// replacement. That is precisely what corrupted three steps of the 2026-09-16
// benchmark matrix, and it is why this exists.
func (r *Relayer) NextNonce(ctx context.Context, addr common.Address) (uint64, error) {
	limits, err := r.cfg.Limits.checked()
	if err != nil {
		return 0, err
	}
	ctx, release, err := r.beginAdmission(ctx, limits)
	if err != nil {
		return 0, err
	}
	defer release()
	if err := r.awaitRecovery(ctx); err != nil {
		return 0, err
	}
	chainNonce, err := r.cfg.Eth.NonceAt(ctx, addr, nil)
	if err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.senders[addr]
	if s == nil {
		return chainNonce, nil
	}
	n := s.next
	if chainNonce > n {
		n = chainNonce
	}
	// Walk past anything already held at or above that nonce, so the answer is
	// a nonce the caller can actually use.
	for {
		if _, ok := s.queued[n]; ok {
			n++
			continue
		}
		if _, ok := s.posted[n]; ok {
			n++
			continue
		}
		return n, nil
	}
}

// SenderStatus is one sender's outstanding work.
type SenderStatus struct {
	Address string `json:"address"`
	Shard   int    `json:"shard"`
	// Next is the lowest nonce not yet posted. Queued entries at or above it
	// go out in order; a gap at Next holds everything behind it.
	Next uint64 `json:"next"`
	// LowestQueued is the smallest queued nonce. When it is above Next this
	// sender is **gapped**: nothing of its can be posted until the missing
	// nonce arrives, and nothing will arrive unless the client resends it.
	LowestQueued uint64 `json:"lowestQueued"`
	Queued       int    `json:"queued"`
	Posted       int    `json:"posted"`
	Gapped       bool   `json:"gapped"`
}

// senderStatuses lists senders with outstanding work, deepest queue first, so a
// status page shows the ones that matter. Caller holds r.mu.
func (r *Relayer) senderStatuses(limit int) []SenderStatus {
	var out []SenderStatus
	for addr, s := range r.senders {
		if len(s.queued) == 0 && len(s.posted) == 0 {
			continue
		}
		st := SenderStatus{Address: addr.Hex(), Shard: s.shard, Next: s.next,
			Queued: len(s.queued), Posted: len(s.posted)}
		first := true
		for n := range s.queued {
			if first || n < st.LowestQueued {
				st.LowestQueued, first = n, false
			}
		}
		st.Gapped = !first && st.LowestQueued > s.next
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Queued > out[j].Queued })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (r *Relayer) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	limits, _ := r.cfg.Limits.checked()
	s := Snapshot{
		RetainedRawBytes: r.retainedBytes, Limits: limits,
		Ready: r.Ready(), RecoveryStatus: r.recoveryStatus(), AdmissionMode: "volatile", AdmissionDurable: false,
		InFlight: append([]int(nil), r.inFlight...), Senders: len(r.senders),
		Received: r.stats.Received.Load(), Rejected: r.stats.Rejected.Load(), Posted: r.stats.Posted.Load(),
		Included: r.stats.Included.Load(), Requeued: r.stats.Requeued.Load(), Discarded: r.stats.Discarded.Load(),
		Batches: r.stats.Batches.Load(), BatchBytes: r.stats.BatchBytes.Load(), RawBytes: r.stats.RawBytes.Load(),
		PublishErrors: r.stats.PublishErrors.Load(),
		Rebids:        r.stats.Rebids.Load(),
		SenderQueues:  r.senderStatuses(10),
	}
	for _, a := range r.cfg.Accounts {
		s.Accounts = append(s.Accounts, a.Address().String())
	}
	s.Queued, s.PostedPending = r.nQueued, r.nPosted
	s.Footprint = r.cfg.Footprint.Status()
	return s
}

// blockRoom is how many bytes of batch this shard's next NoM block can pay for.
//
// A block's plasma is its bytes times the network's price, and under dynamic
// plasma that price moves: at 1.8x a full 16 KB block costs about 2 M plasma,
// so eight of them in flight need 16 M against the 10.5 M that 5,000 fused QSR
// provides — which is how a relayer sized for a v1 network starts being refused
// with "not enough plasma on account" the moment a v2 network gets busy.
// Measured on the chain-71 devnet, 2026-09-15.
//
// So the room is what the account can afford *now*, divided by the blocks it
// allows itself in flight. On a network without a fee market the price is the
// minimum and this comes out at a full block, which is what it always was.
func (r *Relayer) blockRoom(ctx context.Context, shard int) int {
	acct := r.cfg.Accounts[shard]
	info, err := r.cfg.L1.PlasmaInfo(ctx, acct.Address())
	if err != nil {
		if cached := r.cachedRoom(shard); cached > 0 {
			return cached
		}
		return batchRoom
	}
	m, err := r.cfg.L1.FrontierMomentum(ctx)
	if err != nil {
		if cached := r.cachedRoom(shard); cached > 0 {
			return cached
		}
		return batchRoom
	}
	r.l1Height.Store(m.Height)
	price := m.FusionPrice()
	// CurrentPlasma is what is left after the blocks already in flight, so the
	// budget is shared with the slots still free, not with all of them.
	r.mu.Lock()
	free := r.cfg.MaxInFlight - r.inFlight[shard]
	r.mu.Unlock()
	perBlock := info.CurrentPlasma / uint64(max(free, 1))
	affordable := perBlock * znn.PriceScaleFactor / price
	room := batchRoom
	if affordable > znn.AccountBlockBasePlasma {
		if bytes := int((affordable - znn.AccountBlockBasePlasma) / znn.ByteDataPlasma); bytes < room {
			room = bytes
		}
	} else {
		room = 0
	}
	// Below minBlockData a batch is not worth a block. The per-slot share can
	// fall under it while the account could still pay for one, and then it
	// posts one that size; when the whole of its plasma cannot, the answer is
	// 0 and the poster waits for blocks to confirm and hand plasma back. This
	// used to answer 1,024 either way, so a shard with nothing left posted,
	// was refused "not enough plasma", and tried again two seconds later.
	if room < minBlockData {
		room = 0
		if info.CurrentPlasma*znn.PriceScaleFactor/price >= znn.AccountBlockBasePlasma+minBlockData*znn.ByteDataPlasma {
			room = minBlockData
		}
	}
	r.mu.Lock()
	r.room[shard] = room
	if shard < len(r.capacity) {
		r.capacity[shard] = info.MaxPlasma
	}
	r.mu.Unlock()
	return room
}

// minBlockData is the smallest batch worth a NoM block of its own.
const minBlockData = 1024

func (r *Relayer) cachedRoom(shard int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.room[shard]
}

// Ready is admission readiness, not transaction inclusion or finality.
func (r *Relayer) Ready() bool { return r.admissionReady.Load() }

func (r *Relayer) recoveryStatus() string {
	switch r.recoveryPhase.Load() {
	case 1:
		return "complete"
	case 2:
		return "failed"
	case 3:
		return "stopped"
	}
	return "starting"
}

func (r *Relayer) awaitRecovery(ctx context.Context) error {
	if r.adopted == nil {
		return nil
	} // package-local fixtures with no service lifecycle
	select {
	case <-r.adopted:
	case <-ctx.Done():
		return ctx.Err()
	}
	if !r.Ready() {
		return errors.New("relayer admission unavailable: recovery is not complete")
	}
	return nil
}
