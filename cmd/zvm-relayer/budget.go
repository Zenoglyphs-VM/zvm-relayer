package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

const (
	maxRelayRPCBody  = 8 << 20
	maxRelayRPCBatch = 100
)

// Reserve capacity before parsing a request body. GET health/status remains
// available while POST callers are receiving explicit backpressure.
func relayHTTPBudget(next http.Handler, concurrency int, timeout time.Duration) http.Handler {
	if concurrency < 1 || concurrency > 256 || timeout <= 0 || timeout > time.Minute {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "invalid relay request budgets", http.StatusServiceUnavailable)
		})
	}
	slots := make(chan struct{}, concurrency)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "relay capacity is busy", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		// The context bounds upstream RPC work; a socket read deadline also
		// bounds a caller that stops sending halfway through its body.
		controller := http.NewResponseController(w)
		_ = controller.SetReadDeadline(time.Now().Add(timeout))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Read one byte beyond the cap so an oversized request cannot be treated as a
// complete shorter request. No RPC handler runs until the full body is read.
func relayBody(next func(http.ResponseWriter, *http.Request, []byte)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST JSON-RPC here, or GET /status", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxRelayRPCBody+1))
		if err != nil {
			http.Error(w, "request body could not be read", http.StatusBadRequest)
			return
		}
		if len(body) > maxRelayRPCBody {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		next(w, r, body)
	})
}

// relayBatch bounds decoded request objects before constructing the result
// list. A byte cap alone would still allow a huge array of tiny objects.
func relayBatch(body []byte) ([]rpcRequest, *rpcError) {
	invalid := func(code int, message string) ([]rpcRequest, *rpcError) {
		return nil, &rpcError{Code: code, Message: message}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return invalid(-32700, "invalid JSON-RPC batch")
	}
	var requests []rpcRequest
	for decoder.More() {
		if len(requests) >= maxRelayRPCBatch {
			return invalid(-32600, "batch must contain 1..100 requests")
		}
		var request rpcRequest
		if decoder.Decode(&request) != nil {
			return invalid(-32700, "invalid JSON-RPC batch")
		}
		requests = append(requests, request)
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim(']') {
		return invalid(-32700, "invalid JSON-RPC batch")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return invalid(-32700, "trailing JSON-RPC batch data")
	}
	if len(requests) == 0 {
		return invalid(-32600, "batch must contain 1..100 requests")
	}
	return requests, nil
}
