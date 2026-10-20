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
