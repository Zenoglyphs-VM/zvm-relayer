package relayer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/Zenoglyphs-VM/zvm-core/derive"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
)

// recoverDepth is how far back each account's chain is read on startup for
// batches still in flight. MaxInFlight blocks, some of them a batch in chunks
// plus its assemble, fit well inside it.
const recoverDepth = 64

// recovered is one batch block found on an account's chain at startup.
type recovered struct {
	shard   int
	block   *znn.AccountBlock
	bytes   int
	txs     []*types.Transaction
	raws    [][]byte
	senders []common.Address
}

// recoverInFlight adopts the batches this relayer's accounts posted before it
// last stopped whose transactions have not been derived yet, and reads each
// account's plasma so that pickShard is weighted from the first submission.
//
// The relayer keeps nothing on disk, so a restart used to forget what it had in
// flight. Wallets and the bench resubmit whatever they have not seen land; the
// relayer knew nothing of the hash and posted it again while the first copy was
// still on its way, and one of the two was then dropped as "nonce too low". On
// 2026-09-22 the re-post spikes on chain 69 came soon after two restarts.
// Adopted, these batches are tracked and settled like any other, and a
// resubmission is answered from byHash.
func (r *Relayer) recoverInFlight(ctx context.Context) error {
	for shard, acct := range r.cfg.Accounts {
		if info, err := r.cfg.L1.PlasmaInfo(ctx, acct.Address()); err == nil {
			r.mu.Lock()
			r.capacity[shard] = info.MaxPlasma
			r.mu.Unlock()
		}
	}

	l2Head, err := r.cfg.Eth.BlockNumber(ctx)
	if err != nil {
		return err
	}
	var found []*recovered
	for shard, acct := range r.cfg.Accounts {
		fs, err := r.inFlightOn(ctx, shard, acct.Address(), l2Head)
		if err != nil {
			return fmt.Errorf("account %d (%s): %w", shard, acct.Address(), err)
		}
		found = append(found, fs...)
	}
	if len(found) == 0 {
		return nil
	}

	// Each sender's nonce at the head: a transaction below it has landed, or
	// lost its nonce to another, and is not in flight whatever its block says.
	nonces := map[common.Address]uint64{}
	for _, f := range found {
		for _, from := range f.senders {
			if _, ok := nonces[from]; ok {
				continue
			}
			_, a, err := r.headFor(ctx, from)
			if err != nil {
				return err
			}
			nonces[from] = a.nonce
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	var batches, txs int
	for _, f := range found {
		b := &batch{shard: f.shard, block: f.block.Hash, height: f.block.Height, published: now, bytes: f.bytes}
		if d := f.block.ConfirmationDetail; d != nil {
			b.confirmed, b.momentum = true, d.MomentumHeight
		}
		for i, tx := range f.txs {
			from, n := f.senders[i], tx.Nonce()
			if r.byHash[tx.Hash()] != nil || n < nonces[from] {
				continue
			}
			s := r.senders[from]
			if s == nil {
				s = &sender{addr: from, shard: f.shard, next: nonces[from],
					queued: map[uint64]*entry{}, posted: map[uint64]*entry{}}
				r.senders[from] = s
				r.byShard[f.shard][from] = s
			}
			if s.posted[n] != nil {
				continue
			}
			e := &entry{tx: tx, raw: f.raws[i], from: from, received: now, queuedAt: now, state: posted, batch: b}
			s.posted[n] = e
			s.postedGas += tx.Gas()
			s.lastUse = now
			r.byHash[tx.Hash()] = e
			r.nPosted++
			r.load[s.shard]++
			b.entries = append(b.entries, e)
		}
		if len(b.entries) == 0 {
			continue
		}
		r.batches = append(r.batches, b)
		if !b.confirmed {
			r.inFlight[f.shard]++
		}
		batches++
		txs += len(b.entries)
	}
	for _, s := range r.senders {
		s.settleCursor(0)
	}
	r.cfg.Log.Info("adopted batches still in flight from before this start", "batches", batches, "txs", txs)
	return nil
}

// inFlightOn reads the last recoverDepth blocks of one account and returns the
// batches among them whose momentum has not been derived yet.
func (r *Relayer) inFlightOn(ctx context.Context, shard int, addr znn.Address, l2Head uint64) ([]*recovered, error) {
	front, err := r.cfg.L1.FrontierAccountBlock(ctx, addr)
	if err != nil {
		return nil, err
	}
	if front == nil || front.Height == 0 {
		return nil, nil
	}
	from := uint64(1)
	if front.Height > recoverDepth {
		from = front.Height - recoverDepth + 1
	}
	list, err := r.cfg.L1.AccountBlocksByHeight(ctx, addr, from, front.Height-from+1)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, nil
	}
	var out []*recovered
	for _, blk := range list.List {
		if blk == nil || blk.ToAddress != r.cfg.Rollup.Inbox || len(blk.Data) == 0 {
			continue
		}
		if d := blk.ConfirmationDetail; d != nil && l2Head >= r.cfg.Rollup.L2BlockForMomentum(d.MomentumHeight) {
			continue // derived already: settled one way or the other
		}
		payload, err := r.batchPayload(ctx, blk)
		if err != nil {
			r.cfg.Log.Warn("reading a batch at startup", "shard", shard, "block", blk.Hash, "err", err)
			continue
		}
		if payload == nil {
			continue
		}
		raws, err := derive.DecodeBatch(payload, r.cfg.Rollup.MaxBatchBytes, true)
		if err != nil {
			r.cfg.Log.Warn("decoding a batch at startup", "shard", shard, "block", blk.Hash, "err", err)
			continue
		}
		f := &recovered{shard: shard, block: blk, bytes: len(payload) + 1}
		for _, raw := range raws {
			tx := new(types.Transaction)
			if tx.UnmarshalBinary(raw) != nil {
				continue
			}
			sender, err := types.Sender(r.signer, tx)
			if err != nil {
				continue
			}
			f.txs, f.raws, f.senders = append(f.txs, tx), append(f.raws, raw), append(f.senders, sender)
		}
		if len(f.txs) > 0 {
			out = append(out, f)
		}
	}
	return out, nil
}

// batchPayload returns what follows the batch type byte in a block's message:
// the block's own data for a batch, the joined chunks for an assemble of one,
// and nil for anything else — a chunk on its own is carried by the assemble
// that names it.
func (r *Relayer) batchPayload(ctx context.Context, blk *znn.AccountBlock) ([]byte, error) {
	switch blk.Data[0] {
	case derive.MsgBatch:
		return blk.Data[1:], nil
	case derive.MsgAssemble:
		hashes, err := derive.DecodeAssemble(blk.Data[1:])
		if err != nil {
			return nil, err
		}
		var joined []byte
		for _, h := range hashes {
			c, err := r.cfg.L1.AccountBlockByHash(ctx, h)
			if err != nil {
				return nil, err
			}
			if c == nil || len(c.Data) == 0 || c.Data[0] != derive.MsgChunk {
				return nil, errors.New("chunk " + h.String() + " is missing")
			}
			joined = append(joined, c.Data[1:]...)
		}
		if len(joined) == 0 || joined[0] != derive.MsgBatch {
			return nil, nil
		}
		return joined[1:], nil
	}
	return nil, nil
}
