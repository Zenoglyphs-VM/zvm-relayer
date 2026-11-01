package relayer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRecoveryResponseByteBudgets(t *testing.T) {
	for _, streamed := range []bool{false, true} {
		t.Run(fmt.Sprintf("streamed=%t", streamed), func(t *testing.T) {
			var calls atomic.Int32
			payload := strings.Repeat("x", 2048)
			encoded := func(id int) []byte {
				raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": payload})
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			size := int64(len(encoded(1)))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var in struct {
					ID int `json:"id"`
				}
				if json.NewDecoder(r.Body).Decode(&in) != nil {
					t.Error("fixture request refused")
					return
				}
				raw := encoded(in.ID)
				if streamed {
					w.(http.Flusher).Flush()
				} else {
					w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
				}
				_, _ = w.Write(raw)
			}))
			defer server.Close()
			reader := func(single, total int64) *recoveryReader {
				limits, err := (RecoveryLimits{MaxResponseBytes: single, MaxTotalResponseBytes: total}).Checked()
				if err != nil {
					t.Fatal(err)
				}
				return &recoveryReader{limits: limits, client: server.Client(), endpoint: server.URL}
			}
			t.Run("single-response", func(t *testing.T) {
				q := reader(size-1, 4*size)
				out := "unchanged"
				if err := q.read(context.Background(), &out, "fixture"); !errors.Is(err, ErrRecoveryResponseBytes) || out != "unchanged" {
					t.Fatal("individual response overrun was not distinct and fail-closed")
				}
			})
			t.Run("cumulative-response", func(t *testing.T) {
				q := reader(size+1, 2*size-1)
				var out string
				if err := q.read(context.Background(), &out, "fixture"); err != nil || out != payload {
					t.Fatal("first bounded response refused", err)
				}
				out = "unchanged"
				if err := q.read(context.Background(), &out, "fixture"); !errors.Is(err, ErrRecoveryTotalResponseBytes) || out != "unchanged" {
					t.Fatal("aggregate overrun was not distinct and fail-closed")
				}
			})
			t.Run("exact-total-stops-before-transport", func(t *testing.T) {
				before := calls.Load()
				q := reader(size, size)
				var out string
				if err := q.read(context.Background(), &out, "fixture"); err != nil || out != payload || q.responseBytes != size {
					t.Fatal("exact boundary refused", err)
				}
				out = "unchanged"
				if err := q.read(context.Background(), &out, "fixture"); !errors.Is(err, ErrRecoveryTotalResponseBytes) || out != "unchanged" || calls.Load() != before+1 || q.calls != 1 {
					t.Fatal("exhausted aggregate budget made an extra RPC or changed output")
				}
			})
			t.Run("explicit-larger-total-keeps-single-cap", func(t *testing.T) {
				q := reader(size, 2*size)
				for i := 0; i < 2; i++ {
					var out string
					if err := q.read(context.Background(), &out, "fixture"); err != nil || out != payload {
						t.Fatal("operator-selected larger total was not effective", err)
					}
				}
				before := calls.Load()
				var out string
				if err := q.read(context.Background(), &out, "fixture"); !errors.Is(err, ErrRecoveryTotalResponseBytes) || calls.Load() != before {
					t.Fatal("larger total disabled its final bound")
				}
			})
		})
	}
}
