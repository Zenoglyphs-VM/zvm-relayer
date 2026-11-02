package relayer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Limits bound retained work and admission concurrency. Configure them before
// starting the relayer; zero selects a finite default, never unlimited work.
type Limits struct {
	MaxTransactions       int           `json:"maxTransactions"`
	MaxRawBytes           int64         `json:"maxRawBytes"`
	MaxSenders            int           `json:"maxSenders"`
	MaxSenderTransactions int           `json:"maxSenderTransactions"`
	MaxSenderRawBytes     int64         `json:"maxSenderRawBytes"`
	MaxNonceGap           uint64        `json:"maxNonceGap"`
	MaxConcurrent         int           `json:"maxConcurrent"`
	AdmissionTimeout      time.Duration `json:"admissionTimeoutNs"`
}

func DefaultLimits() Limits {
	return Limits{MaxTransactions: 16384, MaxRawBytes: 64 << 20, MaxSenders: 4096, MaxSenderTransactions: 1024, MaxSenderRawBytes: 4 << 20, MaxNonceGap: 4096, MaxConcurrent: 32, AdmissionTimeout: 5 * time.Second}
}

func (l Limits) checked() (Limits, error) {
	d := DefaultLimits()
	if l.MaxTransactions == 0 {
		l.MaxTransactions = d.MaxTransactions
	}
	if l.MaxRawBytes == 0 {
		l.MaxRawBytes = d.MaxRawBytes
	}
	if l.MaxSenders == 0 {
		l.MaxSenders = d.MaxSenders
	}
	if l.MaxSenderTransactions == 0 {
		l.MaxSenderTransactions = d.MaxSenderTransactions
	}
	if l.MaxSenderRawBytes == 0 {
		l.MaxSenderRawBytes = d.MaxSenderRawBytes
	}
	if l.MaxNonceGap == 0 {
		l.MaxNonceGap = d.MaxNonceGap
	}
	if l.MaxConcurrent == 0 {
		l.MaxConcurrent = d.MaxConcurrent
	}
	if l.AdmissionTimeout == 0 {
		l.AdmissionTimeout = d.AdmissionTimeout
	}
	if l.MaxTransactions < 1 || l.MaxRawBytes < 1 || l.MaxSenders < 1 || l.MaxSenderTransactions < 1 || l.MaxSenderRawBytes < 1 || l.MaxConcurrent < 1 || l.MaxConcurrent > 256 || l.AdmissionTimeout < 0 || l.AdmissionTimeout > time.Minute {
		return l, errors.New("relayer limits must be positive; concurrency is at most 256 and admission deadline at most one minute")
	}
	return l, nil
}

func (l Limits) Check() error { _, err := l.checked(); return err }

// Resolve applies finite defaults and rejects invalid resource budgets.
func (l Limits) Resolve() (Limits, error) { return l.checked() }

type CapacityError struct {
	Resource       string
	Current, Limit int64
}

func (e *CapacityError) Error() string {
	return fmt.Sprintf("relayer %s limit reached (%d of %d)", e.Resource, e.Current, e.Limit)
}
func (e *CapacityError) Unwrap() error { return &RejectError{Reason: e.Error()} }

// Callers hold r.mu. Count/byte limits include posted work until settlement,
// so moving a transaction from queued to posted does not free its reservation.
func (r *Relayer) checkCapacity(l Limits, from common.Address, rawBytes int, old *entry) error {
	s := r.senders[from]
	if s == nil && len(r.senders) >= l.MaxSenders {
		return &CapacityError{"senders", int64(len(r.senders)), int64(l.MaxSenders)}
	}
	if old == nil && len(r.byHash) >= l.MaxTransactions {
		return &CapacityError{"transactions", int64(len(r.byHash)), int64(l.MaxTransactions)}
	}
	previous := int64(0)
	if old != nil {
		previous = int64(len(old.raw))
	}
	// Subtract the replaced reservation before comparing the new cost. This
	// never adds past MaxInt64 and never releases the original on refusal.
	cost := int64(rawBytes)
	if cost < 0 || r.retainedBytes < previous || r.retainedBytes < 0 || cost > l.MaxRawBytes-(r.retainedBytes-previous) {
		return &CapacityError{"raw bytes", r.retainedBytes, l.MaxRawBytes}
	}
	count, held := 0, int64(0)
	if s != nil {
		count = len(s.queued) + len(s.posted)
		held = s.rawBytes
	}
	if old == nil && count >= l.MaxSenderTransactions {
		return &CapacityError{"sender transactions", int64(count), int64(l.MaxSenderTransactions)}
	}
	if held < previous || held < 0 || cost > l.MaxSenderRawBytes-(held-previous) {
		return &CapacityError{"sender raw bytes", held, l.MaxSenderRawBytes}
	}
	return nil
}

func (r *Relayer) remember(e *entry) {
	if previous := r.byHash[e.tx.Hash()]; previous != nil {
		r.forget(previous.tx.Hash())
	}
	r.byHash[e.tx.Hash()] = e
	r.retainedBytes += int64(len(e.raw))
	if s := r.senders[e.from]; s != nil {
		s.rawBytes += int64(len(e.raw))
	}
}

func (r *Relayer) forget(hash common.Hash) {
	if e := r.byHash[hash]; e != nil {
		r.retainedBytes -= int64(len(e.raw))
		if s := r.senders[e.from]; s != nil {
			s.rawBytes -= int64(len(e.raw))
		}
		delete(r.byHash, hash)
	}
}

func (r *Relayer) beginAdmission(parent context.Context, l Limits) (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(parent, l.AdmissionTimeout)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, nil, err
	}
	r.admitOnce.Do(func() { r.admission = make(chan struct{}, l.MaxConcurrent) })
	select {
	case r.admission <- struct{}{}:
		return ctx, func() { <-r.admission; cancel() }, nil
	default:
		cancel()
		return nil, nil, &CapacityError{"concurrent admission", int64(l.MaxConcurrent), int64(l.MaxConcurrent)}
	}
}
