package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/Zenoglyphs-VM/zvm-core/devnet"
	"github.com/Zenoglyphs-VM/zvm-core/ethrpc"
	"github.com/Zenoglyphs-VM/zvm-core/rollup"
	"github.com/Zenoglyphs-VM/zvm-core/znn"
	"github.com/Zenoglyphs-VM/zvm-relayer/relayer"
)

func init() {
	register("relayer", "the batch lane: take signed EVM transactions and post them to NoM", runRelayer)
}

func runRelayer(args []string) error {
	fs := flag.NewFlagSet("relayer", flag.ExitOnError)
	configPath := fs.String("config", "/config/rollup.json", "rollup config")
	l1URL := fs.String("l1", "http://l1-rpc:35997", "NoM node JSON-RPC")
	ethURL := fs.String("eth", "http://op-geth:8545", "ZVM JSON-RPC (op-geth), for state checks")
	listen := fs.String("listen", ":8547", "JSON-RPC listen address (op-geth's --rollup.sequencerhttp points here)")
	accounts := fs.Uint("accounts", devnet.NumRelayers, "NoM accounts to post from")
	first := fs.Uint("first-index", devnet.RelayerBase, "dev mnemonic index of the first relayer account")
	maxWait := fs.Duration("max-wait", time.Second, "longest a transaction waits for its batch to fill")
	maxInFlight := fs.Int("max-inflight", 8, "unconfirmed blocks allowed per account")
	queueTTL := fs.Duration("queue-ttl", 5*time.Minute, "how long a transaction may wait for a gap in front of it before it is discarded")
	maxRequeues := fs.Int("max-requeues", 8, "how many times one transaction may be posted again before the relayer gives up")
	maxBatchGas := fs.Uint64("max-batch-gas", 0, "gas one batch may declare (default: what a block admits from the batch lane)")
	footprint := fs.Bool("footprint", true, "keep inside a share of NoM's own capacity, forecast from recent momentums")
	floorBP := fs.Uint64("footprint-floor-bp", relayer.DefaultFootprintFloorBP, "the smallest share of NoM this relayer will take, in basis points")
	ceilingBP := fs.Uint64("footprint-ceiling-bp", relayer.DefaultFootprintCeilingBP, "the largest share of NoM this relayer will take, in basis points")
	minTip := fs.Uint64("min-tip", relayer.DefaultMinTipWei, "smallest priority fee this relayer will carry, wei per gas (0: carry anything)")
	recovery, err := recoveryFlags(fs)
	if err != nil {
		return err
	}
	admission := admissionStoreFlags(fs)
	fs.Parse(args)
	recoveryLimits, err := recovery.Checked()
	if err != nil {
		return err
	}

	admissionLimits, err := admission.Checked()
	if err != nil {
		return err
	}
	if *accounts == 0 || *accounts > 128 {
		return errors.New("relayer account count must be between 1 and 128")
	}
	cfg, err := rollup.Load(*configPath)
	if err != nil {
		return err
	}
	ctx, cancel := signalContext()
	defer cancel()
	eth, err := waitFor(ctx, "ZVM rpc", func() (*ethclient.Client, error) {
		c, err := ethrpc.Dial(ctx, *ethURL)
		if err != nil {
			return nil, err
		}
		_, err = c.BlockNumber(ctx)
		return c, err
	})
	if err != nil {
		return err
	}
	l1 := znn.NewClient(*l1URL)
	var accts []*znn.Account
	for i := uint(0); i < *accounts; i++ {
		a := znn.NewAccount(devnet.Key(uint32(*first+i)), l1, cfg.L1ChainID)
		a.AssumeFused = true
		accts = append(accts, a)
	}
	rc := relayer.Config{
		Recovery: recoveryLimits,
		Rollup:   cfg, L1: l1, Eth: eth, Accounts: accts,
		MaxWait: *maxWait, MaxInFlight: *maxInFlight,
		QueueTTL: *queueTTL, MaxRequeues: *maxRequeues, MaxBatchGas: *maxBatchGas,
		Log: slog.Default().With("svc", "relayer"),
	}
	if *footprint {
		// relayer.New seeds the lane's shared gas budget from the block ceiling
		// once it has resolved it.
		rc.Footprint = relayer.NewFootprint(*floorBP, *ceilingBP)
		if *minTip > 0 {
			rc.MinTip = new(big.Int).SetUint64(*minTip)
		}
	}
	if admission.Directory != "" {
		rc.AdmissionStore, err = relayer.OpenAdmissionStore(admission.Directory, rc, admissionLimits)
		if err != nil {
			return err
		}
		defer rc.AdmissionStore.Close()
	}
	r := relayer.New(rc)

	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, r.Snapshot())
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		if !r.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "POST JSON-RPC here, or GET /status", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(req.Body, 8<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		serveJSONRPC(w, req.Context(), body, func(ctx context.Context, method string, params json.RawMessage) (any, *rpcError) {
			switch method {
			case "eth_sendRawTransaction":
				var ps []string
				if err := json.Unmarshal(params, &ps); err != nil || len(ps) != 1 {
					return nil, &rpcError{Code: -32602, Message: "expected one hex-encoded transaction"}
				}
				raw, err := hexutil.Decode(ps[0])
				if err != nil {
					return nil, &rpcError{Code: -32602, Message: err.Error()}
				}
				hash, err := r.Submit(ctx, raw)
				if err != nil {
					var rej *relayer.RejectError
					if errors.As(err, &rej) {
						return nil, &rpcError{Code: -32000, Message: rej.Reason}
					}
					return nil, &rpcError{Code: -32603, Message: err.Error()}
				}
				return hash, nil
			case "eth_getTransactionCount":
				// The relayer is this chain's mempool, so it is the only thing
				// that can answer a *pending* nonce. These transactions never
				// enter the execution client's pool — they go to NoM — so
				// asking the client returns a nonce this relayer already holds
				// queued, and the next submission is refused as an underpriced
				// replacement. That is what corrupted three steps of the
				// 2026-09-16 benchmark matrix. `latest` stays the chain's own
				// answer, so only a caller that asks for `pending` changes.
				var ps []json.RawMessage
				if err := json.Unmarshal(params, &ps); err != nil || len(ps) == 0 {
					return nil, &rpcError{Code: -32602, Message: "expected an address and a block tag"}
				}
				var addrHex string
				if err := json.Unmarshal(ps[0], &addrHex); err != nil || !common.IsHexAddress(addrHex) {
					return nil, &rpcError{Code: -32602, Message: "expected an address"}
				}
				tag := "latest"
				if len(ps) > 1 {
					_ = json.Unmarshal(ps[1], &tag)
				}
				addr := common.HexToAddress(addrHex)
				if tag != "pending" {
					n, err := eth.NonceAt(ctx, addr, nil)
					if err != nil {
						return nil, &rpcError{Code: -32603, Message: err.Error()}
					}
					return hexutil.Uint64(n), nil
				}
				n, err := r.NextNonce(ctx, addr)
				if err != nil {
					return nil, &rpcError{Code: -32603, Message: err.Error()}
				}
				return hexutil.Uint64(n), nil
			case "eth_chainId":
				return hexutil.Uint64(cfg.L2ChainID), nil
			case "net_version":
				return fmt.Sprint(cfg.L2ChainID), nil
			default:
				return nil, &rpcError{Code: -32601, Message: "the relayer accepts eth_sendRawTransaction and eth_getTransactionCount; read other state from the ZVM RPC"}
			}
		})
	})
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		srv.Shutdown(shutdown)
	}()
	go func() {
		slog.Info("relayer listening", "addr", *listen, "accounts", len(accts))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("relayer http", "err", err)
			cancel()
		}
	}()
	return r.Run(ctx)
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// serveJSONRPC answers a single request or a batch.
func serveJSONRPC(w http.ResponseWriter, ctx context.Context, body []byte,
	handle func(ctx context.Context, method string, params json.RawMessage) (any, *rpcError)) {
	trimmed := strings.TrimSpace(string(body))
	one := func(req rpcRequest) rpcResponse {
		res, rerr := handle(ctx, req.Method, req.Params)
		if req.ID == nil {
			req.ID = json.RawMessage("null")
		}
		return rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: res, Error: rerr}
	}
	if strings.HasPrefix(trimmed, "[") {
		var reqs []rpcRequest
		if err := json.Unmarshal(body, &reqs); err != nil {
			writeJSON(w, http.StatusOK, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{Code: -32700, Message: err.Error()}})
			return
		}
		out := make([]rpcResponse, len(reqs))
		for i, req := range reqs {
			out[i] = one(req)
		}
		writeJSON(w, http.StatusOK, out)
		return
	}
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusOK, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{Code: -32700, Message: err.Error()}})
		return
	}
	writeJSON(w, http.StatusOK, one(req))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
