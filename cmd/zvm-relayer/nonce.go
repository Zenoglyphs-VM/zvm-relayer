package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// nonceBlock validates the selector while keeping its original representation
// for the execution client. Only the named pending state uses queue accounting.
func nonceBlock(raw json.RawMessage) (bool, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return false, errors.New("missing block reference")
	}
	if raw[0] != '{' {
		var tag string
		if json.Unmarshal(raw, &tag) != nil || tag == "" {
			return false, errors.New("block reference must be a string or object")
		}
		switch tag {
		case "pending":
			return true, nil
		case "latest", "earliest", "safe", "finalized":
			return false, nil
		default:
			if _, err := hexutil.DecodeUint64(tag); err != nil {
				return false, errors.New("block number must be a canonical hex quantity")
			}
			return false, nil
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if _, err := decoder.Token(); err != nil {
		return false, err
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return false, err
		}
		key, ok := token.(string)
		if !ok {
			return false, errors.New("invalid block reference key")
		}
		if _, exists := fields[key]; exists {
			return false, errors.New("duplicate block reference key")
		}
		if key != "blockNumber" && key != "blockHash" && key != "requireCanonical" {
			return false, errors.New("unknown block reference key")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return false, err
		}
		fields[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return false, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return false, errors.New("trailing block reference data")
	}
	number, hasNumber := fields["blockNumber"]
	hash, hasHash := fields["blockHash"]
	if hasNumber == hasHash {
		return false, errors.New("name exactly one blockNumber or blockHash")
	}
	if hasNumber {
		var quantity string
		if json.Unmarshal(number, &quantity) != nil {
			return false, errors.New("blockNumber must be a hex quantity")
		}
		if _, err := hexutil.DecodeUint64(quantity); err != nil {
			return false, errors.New("blockNumber must be a canonical hex quantity")
		}
	} else {
		var text string
		if json.Unmarshal(hash, &text) != nil {
			return false, errors.New("blockHash must be 32 bytes of hex data")
		}
		data, err := hexutil.Decode(text)
		if err != nil || len(data) != common.HashLength {
			return false, errors.New("blockHash must be 32 bytes of hex data")
		}
	}
	if canonical, ok := fields["requireCanonical"]; ok {
		if !hasHash {
			return false, errors.New("requireCanonical needs a blockHash")
		}
		var value bool
		if bytes.Equal(bytes.TrimSpace(canonical), []byte("null")) || json.Unmarshal(canonical, &value) != nil {
			return false, errors.New("requireCanonical must be a boolean")
		}
	}
	return false, nil
}

func transactionCount(ctx context.Context, params json.RawMessage, eth *ethclient.Client, pending func(context.Context, common.Address) (uint64, error)) (any, *rpcError) {
	var values []json.RawMessage
	if json.Unmarshal(params, &values) != nil || len(values) != 2 {
		return nil, &rpcError{Code: -32602, Message: "expected an address and a block reference"}
	}
	var address string
	if json.Unmarshal(values[0], &address) != nil || !common.IsHexAddress(address) {
		return nil, &rpcError{Code: -32602, Message: "expected an address"}
	}
	isPending, err := nonceBlock(values[1])
	if err != nil {
		return nil, &rpcError{Code: -32602, Message: "invalid block reference: " + err.Error()}
	}
	addr := common.HexToAddress(address)
	if isPending {
		nonce, err := pending(ctx, addr)
		if err != nil {
			return nil, relayAdmissionError(err)
		}
		return hexutil.Uint64(nonce), nil
	}
	var nonce hexutil.Uint64
	if err := eth.Client().CallContext(ctx, &nonce, "eth_getTransactionCount", addr, values[1]); err != nil {
		var backend rpc.Error
		if errors.As(err, &backend) {
			return nil, &rpcError{Code: backend.ErrorCode(), Message: "execution client refused the requested block reference"}
		}
		return nil, &rpcError{Code: -32603, Message: "execution state query failed"}
	}
	return nonce, nil
}
