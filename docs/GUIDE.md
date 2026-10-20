# zvm-relayer guide

The ZVM's **batch lane**: it speaks Ethereum JSON-RPC to wallets, and posts what
they send to Zenon as account blocks.

A ZVM transaction is an ordinary Zenon account block sent to an inbox address in
a namespace no key can reach. An account holding an EVM key has no Zenon plasma
and no Zenon account, so it cannot publish one itself — this is what stands in
between. MetaMask, Rabby and Foundry point at it exactly as they would point at
a sequencer, and it answers `eth_sendRawTransaction` by packing transactions
into batches and publishing them on NoM from a rotating set of relayer accounts.

It is not a sequencer, and the difference matters: it cannot reorder anything.
Order is whatever NoM commits, so the worst a relayer can do is delay you or
drop you — and you can post the same transaction through any other relayer, or
publish the account block yourself.

```sh
go build ./...
go test ./...

zvm-relayer -l1 http://127.0.0.1:45997 \
            -eth http://127.0.0.1:8545 \
            -config l2/rollup.json -listen :8547
```

| | |
|---|---|
| `POST /` | Ethereum JSON-RPC: `eth_sendRawTransaction` and the calls a wallet needs around it |
| `GET /status` | accounts, their plasma, batches in flight, what the last one cost |

## The knobs that matter

- `-accounts` / `-first-index` — how many NoM accounts it posts from. Each
  account block is serial per account, so this is the concurrency.
- `-max-wait` — the longest a transaction waits for its batch to fill. Lower is
  faster and costs more plasma per transaction.
- `-max-inflight` — unconfirmed blocks allowed per account. A NoM account whose
  block misses a congested momentum **freezes** until that block confirms, so
  this bounds how much a congestion spike can wedge.

Consensus lives in [`zvm-core`](https://github.com/Zenoglyphs-VM/zvm-core); the chain this posts to is
derived by [`zvm-node`](https://github.com/Zenoglyphs-VM/zvm-node).
