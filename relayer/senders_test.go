package relayer

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// A stuck relayer looks like a big `queued` number and nothing moving, and from
// outside there was no way to tell *why*. The distinction that matters is
// whether a sender's queue starts at the nonce it is allowed to post next, or
// above it — because a gap can only be filled by the client resending the
// missing nonce, and no client does. This is what makes that visible.
func TestSenderStatusesShowWhichQueuesAreGapped(t *testing.T) {
	inOrder := common.HexToAddress("0x1111111111111111111111111111111111111111")
	gapped := common.HexToAddress("0x2222222222222222222222222222222222222222")
	idle := common.HexToAddress("0x3333333333333333333333333333333333333333")

	r := &Relayer{senders: map[common.Address]*sender{
		// Queued from exactly `next`: these will go out.
		inOrder: {addr: inOrder, shard: 1, next: 5,
			queued: map[uint64]*entry{5: {}, 6: {}}, posted: map[uint64]*entry{}},
		// Nonce 10 was discarded, so 11..13 can never be posted.
		gapped: {addr: gapped, shard: 2, next: 10,
			queued: map[uint64]*entry{11: {}, 12: {}, 13: {}}, posted: map[uint64]*entry{}},
		// Nothing outstanding: should not be listed at all.
		idle: {addr: idle, shard: 0, queued: map[uint64]*entry{}, posted: map[uint64]*entry{}},
	}}

	got := r.senderStatuses(10)
	if len(got) != 2 {
		t.Fatalf("listed %d senders, want the 2 with outstanding work", len(got))
	}
	// Deepest queue first, so the worst offender is at the top of a status page.
	if got[0].Address != gapped.Hex() {
		t.Errorf("first entry is %s, want the deepest queue %s", got[0].Address, gapped.Hex())
	}
	if !got[0].Gapped {
		t.Errorf("queue starting at %d with next %d is not reported as gapped", got[0].LowestQueued, got[0].Next)
	}
	if got[0].LowestQueued != 11 || got[0].Next != 10 || got[0].Queued != 3 {
		t.Errorf("gapped sender = %+v, want lowestQueued 11, next 10, queued 3", got[0])
	}
	if got[1].Gapped {
		t.Errorf("queue starting at its own next (%d) is reported as gapped", got[1].Next)
	}
}

func TestSenderStatusesRespectTheLimit(t *testing.T) {
	r := &Relayer{senders: map[common.Address]*sender{}}
	for i := 0; i < 25; i++ {
		a := common.BigToAddress(common.Big1)
		a[19] = byte(i)
		r.senders[a] = &sender{addr: a, next: 1, queued: map[uint64]*entry{1: {}}, posted: map[uint64]*entry{}}
	}
	if got := r.senderStatuses(10); len(got) != 10 {
		t.Errorf("returned %d senders, want the limit of 10", len(got))
	}
}
