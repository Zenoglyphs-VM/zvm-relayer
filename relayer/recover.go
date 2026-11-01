package relayer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/Zenoglyphs-VM/zvm-core/derive"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
)

// RecoveryLimits bound a complete startup scan. Exceeding a limit refuses
// startup; a recent suffix is never substituted for the declared account chain.
// These limits cover this recovery reader, not the service's other RPC paths.
type RecoveryLimits struct {
	MaxBlocks             uint64
	MaxCalls              int
	MaxResponseBytes      int64
	MaxTotalResponseBytes int64
	MaxRawBytes           int64
	MaxTransactions       int
	MaxSenders            int
	Timeout               time.Duration
}

// Checked resolves zero-valued fields to their defaults and validates every
// independent bound. Zero never means unlimited; it retains the library default.
func (l RecoveryLimits) Checked() (RecoveryLimits, error) { return l.checked() }

// Stable errors distinguish which operator-selected response budget refused a
// read without exposing an endpoint, a remote diagnostic or signed bytes.
var (
	ErrRecoveryResponseBytes      = errors.New("recovery single response byte limit exceeded")
	ErrRecoveryTotalResponseBytes = errors.New("recovery total response byte limit exceeded")
)

func (l RecoveryLimits) checked() (RecoveryLimits, error) {
	if l.MaxBlocks == 0 {
		l.MaxBlocks = 65536
	}
	if l.MaxCalls == 0 {
		l.MaxCalls = 8192
	}
	if l.MaxResponseBytes == 0 {
		l.MaxResponseBytes = 2 << 20
	}
	if l.MaxTotalResponseBytes == 0 {
		l.MaxTotalResponseBytes = 128 << 20
	}
	if l.MaxRawBytes == 0 {
		l.MaxRawBytes = 64 << 20
	}
	if l.MaxTransactions == 0 {
		l.MaxTransactions = 32768
	}
	if l.MaxSenders == 0 {
		l.MaxSenders = 4096
	}
	if l.Timeout == 0 {
		l.Timeout = 2 * time.Minute
	}
	if l.MaxBlocks > 16<<20 || l.MaxCalls < 1 || l.MaxCalls > 1<<20 ||
		l.MaxResponseBytes < 1024 || l.MaxResponseBytes > 16<<20 ||
		l.MaxTotalResponseBytes < l.MaxResponseBytes || l.MaxTotalResponseBytes > 1<<30 ||
		l.MaxRawBytes < 1 || l.MaxRawBytes > 512<<20 ||
		l.MaxTransactions < 1 || l.MaxTransactions > 1<<20 || l.MaxSenders < 1 || l.MaxSenders > 1<<20 ||
		l.Timeout < time.Millisecond || l.Timeout > 30*time.Minute {
		return l, errors.New("invalid recovery limits")
	}
	return l, nil
}

type recoveryReader struct {
	limits        RecoveryLimits
	client        *http.Client
	endpoint      string
	calls         int
	responseBytes int64
}

func uniqueRecoveryEnvelope(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		key, ok := token.(string)
		if !ok || seen[key] || key != "jsonrpc" && key != "id" && key != "result" && key != "error" {
			return false
		}
		seen[key] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return false
		}
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') {
		return false
	}
	var extra any
	return decoder.Decode(&extra) == io.EOF
}

// read uses a bounded body and an exact response envelope. Error text never
// exports endpoint credentials, a remote diagnostic or signed transaction bytes.
func (q *recoveryReader) read(ctx context.Context, out any, method string, params ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	remaining := q.limits.MaxTotalResponseBytes - q.responseBytes
	if remaining <= 0 {
		return ErrRecoveryTotalResponseBytes
	}
	cap := min(q.limits.MaxResponseBytes, remaining)
	byteLimitError := ErrRecoveryResponseBytes
	if remaining < q.limits.MaxResponseBytes {
		byteLimitError = ErrRecoveryTotalResponseBytes
	}
	if q.calls >= q.limits.MaxCalls {
		return errors.New("recovery RPC call limit exceeded")
	}
	q.calls++
	if params == nil {
		params = []any{}
	}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": q.calls, "method": method, "params": params})
	if err != nil {
		return errors.New("recovery request cannot be encoded")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, q.endpoint, bytes.NewReader(request))
	if err != nil {
		return errors.New("invalid recovery endpoint")
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := q.client.Do(req)
	if err != nil {
		return errors.New("recovery RPC transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("recovery RPC HTTP status refused")
	}
	if response.ContentLength > cap {
		return byteLimitError
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, cap+1))
	q.responseBytes += int64(len(raw))
	if int64(len(raw)) > cap {
		return byteLimitError
	}
	if err != nil {
		return errors.New("recovery response incomplete")
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      int             `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if !uniqueRecoveryEnvelope(raw) || json.Unmarshal(raw, &envelope) != nil || envelope.JSONRPC != "2.0" || envelope.ID != q.calls ||
		len(envelope.Result) == 0 || len(envelope.Error) != 0 && !bytes.Equal(bytes.TrimSpace(envelope.Error), []byte("null")) {
		return errors.New("recovery RPC envelope refused")
	}
	if err := json.Unmarshal(envelope.Result, out); err != nil {
		return errors.New("recovery RPC result refused")
	}
	return nil
}

type recovered struct {
	shard   int
	block   *znn.AccountBlock
	bytes   int
	txs     []*types.Transaction
	raws    [][]byte
	senders []common.Address
}

func (r *Relayer) recoveryBlock(b *znn.AccountBlock, addr znn.Address) error {
	if b == nil || b.Address != addr || b.Height == 0 || b.ChainIdentifier != r.cfg.Rollup.L1ChainID ||
		b.Hash == (znn.Hash{}) || len(b.Data) > znn.MaxDataLength || len(b.DescendantBlocks) > 1024 {
		return errors.New("recovery account block identity or contents refused")
	}
	if amount := b.Amount.Big(); amount.Sign() < 0 || amount.BitLen() > 256 {
		return errors.New("recovery account amount refused")
	}
	for _, descendant := range b.DescendantBlocks {
		if descendant == nil {
			return errors.New("recovery descendant refused")
		}
	}
	if b.Hash != b.ComputeHash() {
		return errors.New("recovery account block hash refused")
	}
	if b.ConfirmationDetail != nil && b.ConfirmationDetail.MomentumHeight == 0 {
		return errors.New("recovery confirmation height refused")
	}
	return nil
}

func (r *Relayer) recoveryPayload(ctx context.Context, q *recoveryReader, b *znn.AccountBlock, seen map[znn.Hash]uint64) ([]byte, error) {
	if b.ToAddress != r.cfg.Rollup.Inbox || len(b.Data) == 0 {
		return nil, nil
	}
	switch b.Data[0] {
	case derive.MsgBatch:
		if len(b.Data) > r.cfg.Rollup.MaxAssembledBytes {
			return nil, errors.New("recovery batch exceeds assembled byte limit")
		}
		return b.Data[1:], nil
	case derive.MsgAssemble:
		hashes, err := derive.DecodeAssemble(b.Data[1:])
		if err != nil {
			return nil, errors.New("recovery assembly refused")
		}
		var joined []byte
		unique := map[znn.Hash]bool{}
		for _, hash := range hashes {
			height, ok := seen[hash]
			if !ok || height >= b.Height || unique[hash] {
				return nil, errors.New("recovery assembly is not bound to earlier account history")
			}
			unique[hash] = true
			var chunk *znn.AccountBlock
			if err := q.read(ctx, &chunk, "ledger.getAccountBlockByHash", hash); err != nil {
				return nil, err
			}
			if err := r.recoveryBlock(chunk, b.Address); err != nil {
				return nil, err
			}
			if chunk.Hash != hash || chunk.Height != height || chunk.ToAddress != r.cfg.Rollup.Inbox || len(chunk.Data) < 2 || chunk.Data[0] != derive.MsgChunk ||
				len(chunk.Data)-1 > r.cfg.Rollup.MaxAssembledBytes-len(joined) {
				return nil, errors.New("recovery chunk missing, changed or over byte limit")
			}
			joined = append(joined, chunk.Data[1:]...)
		}
		if len(joined) == 0 || joined[0] != derive.MsgBatch {
			return nil, errors.New("recovery assembly is not a batch")
		}
		return joined[1:], nil
	}
	return nil, nil
}

// recoverInFlight scans every configured account from height one to a frozen
// frontier, pins execution nonces to one canonical hash, and stages everything
// before changing live state. Configured RPC consistency is not finality proof.
func (r *Relayer) recoverInFlight(parent context.Context) error {
	limits, err := r.cfg.Recovery.checked()
	if err != nil {
		return err
	}
	if r.cfg.Rollup == nil || r.cfg.L1 == nil || r.cfg.Eth == nil || len(r.cfg.Accounts) == 0 || len(r.cfg.Accounts) > 128 ||
		r.cfg.Rollup.L1ChainID == 0 || r.cfg.Rollup.L2ChainID == 0 || r.cfg.Rollup.L1StartHeight == 0 || r.cfg.Rollup.MaxBatchBytes < 1 ||
		r.cfg.Rollup.MaxBatchBytes > 64<<20 || r.cfg.Rollup.MaxAssembledBytes < 1 || r.cfg.Rollup.MaxAssembledBytes > 64<<20 {
		return errors.New("incomplete recovery configuration")
	}
	ctx, cancel := context.WithTimeout(parent, limits.Timeout)
	defer cancel()
	q := &recoveryReader{limits: limits, client: &http.Client{Timeout: limits.Timeout}, endpoint: r.cfg.L1.URL()}
	chainID, err := r.cfg.Eth.ChainID(ctx)
	if err != nil || chainID == nil || chainID.Cmp(r.cfg.Rollup.L2ChainIDBig()) != 0 {
		return errors.New("recovery execution chain identity refused")
	}
	anchor, err := r.cfg.Eth.HeaderByNumber(ctx, nil)
	if err != nil || anchor == nil || anchor.Number == nil || !anchor.Number.IsUint64() {
		return errors.New("recovery execution anchor unavailable")
	}
	if r.cfg.Rollup.L2GenesisHash != (common.Hash{}) {
		genesis, err := r.cfg.Eth.HeaderByNumber(ctx, common.Big0)
		if err != nil || genesis == nil || genesis.Hash() != r.cfg.Rollup.L2GenesisHash {
			return errors.New("recovery execution genesis refused")
		}
	}
	type fence struct {
		address znn.Address
		block   *znn.AccountBlock
	}
	var fences []fence
	var found []*recovered
	capacities := make([]uint64, len(r.cfg.Accounts))
	addresses := map[znn.Address]bool{}
	senders := map[common.Address]bool{}
	nonces := map[common.Address]uint64{}
	var blocks uint64
	var rawBytes int64
	transactions := 0
	for shard, account := range r.cfg.Accounts {
		if account == nil || addresses[account.Address()] {
			return errors.New("duplicate or missing recovery account")
		}
		addr := account.Address()
		addresses[addr] = true
		var plasma znn.PlasmaInfo
		if err := q.read(ctx, &plasma, "embedded.plasma.get", addr); err != nil {
			return err
		}
		capacities[shard] = plasma.MaxPlasma
		var front *znn.AccountBlock
		if err := q.read(ctx, &front, "ledger.getFrontierAccountBlock", addr); err != nil {
			return err
		}
		fences = append(fences, fence{addr, front})
		if front == nil {
			continue
		}
		if err := r.recoveryBlock(front, addr); err != nil {
			return err
		}
		if front.Height > limits.MaxBlocks-blocks {
			return errors.New("complete recovery account history exceeds block limit")
		}
		seen := make(map[znn.Hash]uint64)
		var previous znn.Hash
		for from := uint64(1); from <= front.Height; {
			if err := ctx.Err(); err != nil {
				return err
			}
			count := min(uint64(64), front.Height-from+1)
			var page *znn.BlockList
			if err := q.read(ctx, &page, "ledger.getAccountBlocksByHeight", addr, from, count); err != nil {
				return err
			}
			// go-zenon's count is the full frontier height, not page length.
			if page == nil || page.Count != front.Height || page.More || uint64(len(page.List)) != count {
				return errors.New("incomplete or changed recovery page")
			}
			for i, block := range page.List {
				if err := r.recoveryBlock(block, addr); err != nil {
					return err
				}
				if block.Height != from+uint64(i) || block.PreviousHash != previous || seen[block.Hash] != 0 {
					return errors.New("recovery account history is not contiguous")
				}
				previous = block.Hash
				seen[block.Hash] = block.Height
				blocks++
				if block.ConfirmationDetail != nil && anchor.Number.Uint64() >= r.cfg.Rollup.L2BlockForMomentum(block.ConfirmationDetail.MomentumHeight) {
					continue
				}
				payload, err := r.recoveryPayload(ctx, q, block, seen)
				if err != nil {
					return err
				}
				if payload == nil {
					continue
				}
				raws, err := derive.DecodeBatch(payload, r.cfg.Rollup.MaxBatchBytes, true)
				if err != nil {
					return errors.New("unresolved recovery batch cannot be decoded")
				}
				item := &recovered{shard: shard, block: block, bytes: len(payload) + 1}
				for _, raw := range raws {
					rawBytes += int64(len(raw))
					transactions++
					if rawBytes > limits.MaxRawBytes || transactions > limits.MaxTransactions {
						return errors.New("recovery transaction retention limit exceeded")
					}
					tx := new(types.Transaction)
					if tx.UnmarshalBinary(raw) != nil || tx.Protected() && tx.ChainId().Cmp(r.cfg.Rollup.L2ChainIDBig()) != 0 {
						return errors.New("recovery transaction identity refused")
					}
					sender, err := types.Sender(r.signer, tx)
					if err != nil {
						return errors.New("recovery transaction signature refused")
					}
					senders[sender] = true
					if len(senders) > limits.MaxSenders {
						return errors.New("recovery sender limit exceeded")
					}
					item.txs = append(item.txs, tx)
					item.raws = append(item.raws, bytes.Clone(raw))
					item.senders = append(item.senders, sender)
				}
				if len(item.txs) > 0 {
					found = append(found, item)
				}
			}
			from += count
		}
		if previous != front.Hash {
			return errors.New("recovery history does not reach its frozen frontier")
		}
	}
	selector := map[string]any{"blockHash": anchor.Hash(), "requireCanonical": true}
	for sender := range senders {
		var nonce hexutil.Uint64
		if err := r.cfg.Eth.Client().CallContext(ctx, &nonce, "eth_getTransactionCount", sender, selector); err != nil {
			return errors.New("recovery canonical nonce unavailable")
		}
		nonces[sender] = uint64(nonce)
	}
	for _, saved := range fences {
		var actual *znn.AccountBlock
		if err := q.read(ctx, &actual, "ledger.getFrontierAccountBlock", saved.address); err != nil {
			return err
		}
		if (actual == nil) != (saved.block == nil) {
			return errors.New("recovery account frontier changed")
		}
		if actual != nil {
			if err := r.recoveryBlock(actual, saved.address); err != nil {
				return err
			}
			if actual.Hash != saved.block.Hash || actual.Height != saved.block.Height {
				return errors.New("recovery account frontier changed")
			}
		}
	}
	current, err := r.cfg.Eth.HeaderByNumber(ctx, anchor.Number)
	if err != nil || current == nil || current.Hash() != anchor.Hash() {
		return errors.New("recovery execution anchor changed")
	}
	staged := &Relayer{senders: map[common.Address]*sender{}, byHash: map[common.Hash]*entry{},
		byShard: make([]map[common.Address]*sender, len(r.cfg.Accounts)), load: make([]int, len(r.cfg.Accounts)),
		inFlight: make([]int, len(r.cfg.Accounts)), capacity: capacities}
	for i := range staged.byShard {
		staged.byShard[i] = map[common.Address]*sender{}
	}
	now := time.Now()
	return r.installRecovery(ctx, staged, found, nonces, now)
}

// installRecovery constructs the complete replacement before taking the lock;
// a failed late account, nonce or collision check preserves all live maps.
func (r *Relayer) installRecovery(ctx context.Context, staged *Relayer, found []*recovered, nonces map[common.Address]uint64, now time.Time) error {
	for _, item := range found {
		b := &batch{shard: item.shard, block: item.block.Hash, height: item.block.Height, published: now, bytes: item.bytes}
		if detail := item.block.ConfirmationDetail; detail != nil {
			b.confirmed, b.momentum = true, detail.MomentumHeight
		}
		for i, tx := range item.txs {
			from, nonce := item.senders[i], tx.Nonce()
			if nonce < nonces[from] {
				continue
			}
			s := staged.senders[from]
			if s == nil {
				s = &sender{addr: from, shard: item.shard, next: nonces[from], queued: map[uint64]*entry{}, posted: map[uint64]*entry{}}
				staged.senders[from] = s
				staged.byShard[item.shard][from] = s
			}
			if staged.byHash[tx.Hash()] != nil || s.posted[nonce] != nil || s.shard != item.shard {
				return errors.New("ambiguous recovered sender nonce or transaction")
			}
			if tx.Gas() > math.MaxUint64-s.postedGas {
				return errors.New("recovery gas total overflow")
			}
			e := &entry{tx: tx, raw: item.raws[i], from: from, received: now, queuedAt: now, state: posted, batch: b}
			s.posted[nonce] = e
			s.postedGas += tx.Gas()
			s.lastUse = now
			staged.byHash[tx.Hash()] = e
			staged.nPosted++
			staged.load[item.shard]++
			b.entries = append(b.entries, e)
		}
		if len(b.entries) == 0 {
			continue
		}
		staged.batches = append(staged.batches, b)
		if !b.confirmed {
			staged.inFlight[item.shard]++
		}
	}
	for _, s := range staged.senders {
		s.settleCursor(0)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.byHash) != 0 || len(r.senders) != 0 || len(r.batches) != 0 {
		return errors.New("recovery requires an empty startup lane")
	}
	r.senders, r.byHash, r.byShard, r.batches = staged.senders, staged.byHash, staged.byShard, staged.batches
	r.load, r.inFlight, r.capacity, r.nPosted = staged.load, staged.inFlight, staged.capacity, staged.nPosted
	r.nQueued = 0
	r.cfg.Log.Info("complete account history recovery finished", "batches", len(r.batches), "transactions", r.nPosted)
	return nil
}
