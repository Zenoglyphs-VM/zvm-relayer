package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
)

const fixtureNonceAddress = "0x0000000000000000000000000000000000000007"

func TestHistoricalNonceSelectorsReachTheirRequestedState(t *testing.T) {
	var calls, pendingCalls atomic.Int32
	references := make(chan string, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if json.NewDecoder(r.Body).Decode(&req) != nil || req.Method != "eth_getTransactionCount" || len(req.Params) != 2 {
			t.Error("unexpected fixture request")
			return
		}
		var compact bytes.Buffer
		if json.Compact(&compact, req.Params[1]) != nil {
			t.Error("invalid forwarded selector")
			return
		}
		references <- compact.String()
		n := calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": hexutil.Uint64(n)})
	}))
	defer srv.Close()
	client, err := ethclient.Dial(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	pending := func(context.Context, common.Address) (uint64, error) { pendingCalls.Add(1); return 99, nil }
	hash := "0x" + strings.Repeat("01", 32)
	for _, tc := range []struct{ name, ref string }{
		{"latest", `"latest"`}, {"earliest", `"earliest"`}, {"safe", `"safe"`}, {"finalized", `"finalized"`},
		{"number", `"0x2"`}, {"number_object", `{"blockNumber":"0x2"}`}, {"hash", `{"blockHash":"` + hash + `"}`},
		{"canonical_hash", `{"blockHash":"` + hash + `","requireCanonical":true}`}, {"noncanonical_allowed", `{"blockHash":"` + hash + `","requireCanonical":false}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			got, rejection := transactionCount(context.Background(), json.RawMessage(`["`+fixtureNonceAddress+`",`+tc.ref+`]`), client, pending)
			if rejection != nil || got != hexutil.Uint64(before+1) || calls.Load() != before+1 || pendingCalls.Load() != 0 {
				t.Fatalf("requested execution state was not used: result=%v error=%v", got, rejection)
			}
			if ref := <-references; ref != tc.ref {
				t.Fatalf("selector or canonicality changed: %s", ref)
			}
		})
	}
	t.Run("pending_only_uses_queue", func(t *testing.T) {
		before := calls.Load()
		got, rejection := transactionCount(context.Background(), json.RawMessage(`["`+fixtureNonceAddress+`","pending"]`), client, pending)
		if rejection != nil || got != hexutil.Uint64(99) || calls.Load() != before || pendingCalls.Load() != 1 {
			t.Fatal("pending did not exclusively use queue accounting")
		}
	})
}

func TestMalformedNonceSelectorsCannotBecomeLatest(t *testing.T) {
	hash := "0x" + strings.Repeat("01", 32)
	refs := []struct{ name, raw string }{
		{"null", `null`}, {"number", `5`}, {"boolean", `true`}, {"array", `[]`}, {"unknown_tag", `"unknown"`},
		{"leading_zero", `"0x01"`}, {"empty_hex", `"0x"`}, {"negative", `"-0x1"`}, {"bare_hash", `"` + hash + `"`},
		{"empty_object", `{}`}, {"unknown_field", `{"unknown":true}`}, {"object_tag", `{"blockNumber":"pending"}`},
		{"both_fields", `{"blockNumber":"0x1","blockHash":"` + hash + `"}`},
		{"null_hash_and_number", `{"blockHash":null,"blockNumber":"0x1"}`}, {"null_number", `{"blockNumber":null}`},
		{"null_hash", `{"blockHash":null}`}, {"short_hash", `{"blockHash":"0x01"}`},
		{"canonical_without_hash", `{"requireCanonical":true}`}, {"canonical_with_number", `{"blockNumber":"0x1","requireCanonical":false}`},
		{"duplicate_number", `{"blockNumber":"0x1","blockNumber":"0x2"}`},
		{"duplicate_hash", `{"blockHash":"` + hash + `","blockHash":"` + hash + `"}`},
		{"null_canonical", `{"blockHash":"` + hash + `","requireCanonical":null}`},
		{"string_canonical", `{"blockHash":"` + hash + `","requireCanonical":"true"}`},
	}
	for _, tc := range refs {
		t.Run(tc.name, func(t *testing.T) {
			_, rejection := transactionCount(context.Background(), json.RawMessage(`["`+fixtureNonceAddress+`",`+tc.raw+`]`), nil, nil)
			if rejection == nil || rejection.Code != -32602 {
				t.Fatal("malformed selector reached an absent backend or queue")
			}
		})
	}
	for _, params := range []string{`[]`, `["` + fixtureNonceAddress + `"]`, `["` + fixtureNonceAddress + `","latest",0]`, `["invalid","latest"]`} {
		if _, rejection := transactionCount(context.Background(), json.RawMessage(params), nil, nil); rejection == nil || rejection.Code != -32602 {
			t.Fatal("malformed nonce parameters were accepted")
		}
	}
}

func TestNonceBackendErrorsRetainTheirStateScope(t *testing.T) {
	for _, code := range []int{-32001, -32000, -32601} {
		t.Run(map[int]string{-32001: "missing_state", -32000: "not_canonical", -32601: "unsupported"}[code], func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var q struct {
					ID json.RawMessage `json:"id"`
				}
				_ = json.NewDecoder(r.Body).Decode(&q)
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": q.ID, "error": map[string]any{"code": code, "message": "private fixture diagnostic"}})
			}))
			defer srv.Close()
			client, err := ethclient.Dial(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			_, rejection := transactionCount(context.Background(), json.RawMessage(`["`+fixtureNonceAddress+`",{"blockHash":"0x`+strings.Repeat("01", 32)+`","requireCanonical":true}]`), client, nil)
			if rejection == nil || rejection.Code != code || calls.Load() != 1 || strings.Contains(rejection.Message, "private fixture") {
				t.Fatal("backend error lost its scope, leaked diagnostics or retried latest")
			}
		})
	}
	t.Run("pending_error_has_no_execution_fallback", func(t *testing.T) {
		_, rejection := transactionCount(context.Background(), json.RawMessage(`["`+fixtureNonceAddress+`","pending"]`), nil,
			func(context.Context, common.Address) (uint64, error) {
				return 0, errors.New("private queue diagnostic")
			})
		if rejection == nil || rejection.Code != -32603 || strings.Contains(rejection.Message, "private queue") {
			t.Fatal("pending failure became a latest-state fallback or leaked diagnostics")
		}
	})
}
