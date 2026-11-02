package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zenoglyphs-VM/zvm-relayer/relayer"
	"github.com/ethereum/go-ethereum/common"
)

type countedRelayBody struct{ reads atomic.Int32 }

func (b *countedRelayBody) Read([]byte) (int, error) { b.reads.Add(1); return 0, io.EOF }
func (b *countedRelayBody) Close() error             { return nil }

func TestHTTPBackpressurePrecedesBodyReadsAndPreservesHealth(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var admitted atomic.Int32
	h := relayHTTPBudget(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			return
		}
		if admitted.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}), 1, time.Second)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first request did not reserve its slot")
	}
	body := new(countedRelayBody)
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = body
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" || body.reads.Load() != 0 || admitted.Load() != 1 {
		t.Fatal("excess request consumed body/handler work instead of backpressure")
	}
	health := httptest.NewRecorder()
	h.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK {
		t.Fatal("admission congestion hid health")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("completed request did not release")
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	if admitted.Load() != 2 {
		t.Fatal("completed request retained an admission slot")
	}
}

func TestHTTPDeadlineReachesWorkAndReleasesItsSlot(t *testing.T) {
	h := relayHTTPBudget(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			w.WriteHeader(http.StatusGatewayTimeout)
		case <-time.After(3 * time.Second):
			t.Error("request deadline was not propagated")
		}
	}), 1, 20*time.Millisecond)
	for i := 0; i < 2; i++ {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", nil))
		if response.Code != http.StatusGatewayTimeout {
			t.Fatal("deadline did not cancel work or its prior slot was retained")
		}
	}
}

func TestRPCBodyOverflowNeverExecutesATruncatedRequest(t *testing.T) {
	var calls atomic.Int32
	h := relayBody(func(w http.ResponseWriter, _ *http.Request, body []byte) {
		calls.Add(1)
		if len(body) != maxRelayRPCBody {
			t.Errorf("accepted body length changed: %d", len(body))
		}
		w.WriteHeader(http.StatusOK)
	})
	for _, extra := range []int{0, 1} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(strings.Repeat(" ", maxRelayRPCBody+extra))))
		want := http.StatusOK
		if extra != 0 {
			want = http.StatusRequestEntityTooLarge
		}
		if response.Code != want {
			t.Fatalf("body boundary returned %d, want %d", response.Code, want)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("overflow ran a handler with a truncated request")
	}
}

func TestSlowSocketBodyCannotHoldAnAdmissionSlotIndefinitely(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(relayHTTPBudget(relayBody(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}), 1, 75*time.Millisecond))
	defer srv.Close()
	conn, err := net.DialTimeout("tcp", srv.Listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(conn, "POST / HTTP/1.1\r\nHost: fixture.invalid\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("slow body did not receive its bounded response: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || calls.Load() != 0 {
		t.Fatal("incomplete body executed RPC work")
	}
	complete, err := srv.Client().Post(srv.URL, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	complete.Body.Close()
	if complete.StatusCode != http.StatusOK || calls.Load() != 1 {
		t.Fatal("slow body retained its reservation after the deadline")
	}
}

func TestEmptyAndOversizedRPCBatchesDoNoWork(t *testing.T) {
	for _, count := range []int{0, maxRelayRPCBatch, maxRelayRPCBatch + 1} {
		var calls int
		request := `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`
		batch := "[" + strings.TrimSuffix(strings.Repeat(request+",", count), ",") + "]"
		response := httptest.NewRecorder()
		serveJSONRPC(response, context.Background(), []byte(batch), func(context.Context, string, json.RawMessage) (any, *rpcError) { calls++; return "0x1", nil })
		if count == maxRelayRPCBatch {
			if calls != count {
				t.Fatal("a batch at its supported boundary did not run")
			}
			continue
		}
		var rejection rpcResponse
		if err := json.Unmarshal(response.Body.Bytes(), &rejection); err != nil || calls != 0 || rejection.Error == nil || rejection.Error.Code != -32600 {
			t.Fatalf("invalid batch executed work: calls=%d error=%v", calls, err)
		}
	}
}

func TestInvalidRelayBudgetsFailBeforeLoadingConfiguration(t *testing.T) {
	for _, args := range [][]string{{"-max-transactions=-1"}, {"-max-admission=257"}, {"-admission-timeout=-1s"}, {"-admission-timeout=61s"}, {"-accounts=0"}, {"-accounts=129"}, {"-request-timeout=0"}, {"-request-timeout=61s"}} {
		if err := runRelayer(append(args, "-config=fixture-must-not-be-opened.invalid")); err == nil || strings.Contains(err.Error(), "fixture-must-not-be-opened") {
			t.Fatalf("invalid budgets reached configuration/network setup: %v", err)
		}
	}
}

func TestAdmissionErrorsPreserveCapacityCodesAndHidePrivateTransportDetails(t *testing.T) {
	capacity := relayAdmissionError(fmt.Errorf("wrapped: %w", &relayer.CapacityError{Resource: "transactions", Current: 1, Limit: 1}))
	if capacity.Code != -32005 {
		t.Fatal("capacity refusal was reduced to a generic rejection")
	}
	if rejected := relayAdmissionError(&relayer.RejectError{Reason: "nonce too low"}); rejected.Code != -32000 || rejected.Message != "nonce too low" {
		t.Fatal("expected transaction rejection lost its reason")
	}
	private := relayAdmissionError(errors.New("Post https://operator:fixture-secret@backend.invalid: fixture transport failure"))
	if private.Code != -32603 || strings.Contains(private.Message, "fixture-secret") || strings.Contains(private.Message, "backend.invalid") {
		t.Fatal("private transport details reached the caller")
	}
}

func TestPendingNonceCapacityKeepsItsTypedRPCCode(t *testing.T) {
	_, rejection := transactionCount(context.Background(), json.RawMessage(`["0x0000000000000000000000000000000000000007","pending"]`), nil,
		func(context.Context, common.Address) (uint64, error) {
			return 0, &relayer.CapacityError{Resource: "concurrent admission", Current: 1, Limit: 1}
		})
	if rejection == nil || rejection.Code != -32005 {
		t.Fatal("pending nonce capacity refusal lost its RPC code")
	}
}

func TestOversizedRPCBatchDoesNotAllocateEveryTinyObject(t *testing.T) {
	small := []byte("[" + strings.TrimSuffix(strings.Repeat("{},", maxRelayRPCBatch+1), ",") + "]")
	large := []byte("[" + strings.TrimSuffix(strings.Repeat("{},", 100000), ",") + "]")
	measure := func(body []byte) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := 0; i < 3; i++ {
			requests, rejection := relayBatch(body)
			if len(requests) != 0 || rejection == nil || rejection.Code != -32600 {
				t.Fatal("oversized batch was not refused before work")
			}
		}
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	baseline, actual := measure(small), measure(large)
	if actual > baseline+64<<10 {
		t.Fatal("batch decoder allocated in proportion to refused item count")
	}
}
