# zvm-relayer

The ZVM's batch lane. It speaks Ethereum JSON-RPC to wallets and posts what
they send to Zenon as account blocks.

An EVM key has no Zenon account and no plasma, so it cannot publish a ZVM
transaction itself. MetaMask, Rabby and Foundry point here as they would at a
sequencer. It is not one: order is whatever NoM commits, so a relayer can delay
or drop a transaction, never reorder it, and any other relayer can carry it.

```sh
git clone --recurse-submodules https://github.com/Zenoglyphs-VM/zvm-relayer
go build ./...
go test ./...
docker build -t zvm-relayer .

zvm-relayer -l1 http://127.0.0.1:45997 \
            -eth http://127.0.0.1:8545 \
            -config l2/rollup.json -listen :8547
```

| | |
|---|---|
| `POST /` | `eth_sendRawTransaction` and the calls a wallet needs around it |
| `GET /status` | accounts, plasma, batches in flight |

`-accounts` sets the concurrency, `-max-wait` trades latency for plasma, and
`-max-inflight` bounds how much a congested momentum can wedge an account. No
node is built in: pass `-l1` and `-eth`.

Consensus lives in [zvm-core](https://github.com/Zenoglyphs-VM/zvm-core), a
submodule here.

MIT licence. The full guide, with how it works and why:
[docs/GUIDE.md](docs/GUIDE.md).


## Historical nonce queries

`eth_getTransactionCount` requires an address and block selector. Only `pending`
adds the relayer queue's accounting. Named historical states and canonical hex
block numbers within the supported uint64 range are forwarded to the execution
client. [EIP-1898](https://eips.ethereum.org/EIPS/eip-1898) objects name exactly one
`blockNumber` or 32-byte `blockHash`; a hash may carry a boolean
`requireCanonical`. The original selector and canonicality choice are preserved.
Malformed selectors fail explicitly. Missing-state, noncanonical-state and
unsupported-method errors retain the execution client's error codes; a failed
historical query is never retried against `latest`.

The bounded CI component/race lane uses controlled read-only HTTP dependencies.
The separate Node routing job pins production Node and Core source and adds the
declared test-only overlay; no Node runtime, dependency or published ref changes.
These fixtures do not qualify real archive-provider history, independent
canonicality/finality, wallet behavior or production deployment. Admission limits,
durability and complete recovery remain separate R01/R02 requirements.
