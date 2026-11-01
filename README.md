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


## Admission and restart contract

A successful `eth_sendRawTransaction` result acknowledges a **volatile queue**.
It is not a disk journal, inclusion, execution, canonicality or finality promise.
The client must retain the exact signed bytes, hash, chain/configuration and
account context until the original transaction has a resolved outcome. After an
ambiguous response or crash, preserve that identity and check the original hash
and nonce before any explicitly authorized retry of the identical bytes. Do not
automatically sign a replacement, select a new nonce or switch chain context.

Before accepting submissions or pending-nonce queries, startup now scans every
configured relayer account from height one to a frozen frontier in pages of at
most 64 blocks. It checks contiguous heights, account and chain identity,
computed block hashes, predecessor links, complete pages, assembly ancestry and
unchanged frontiers. Execution nonces are read at one hash with
`requireCanonical: true`; that anchor is checked again before atomic adoption.
Missing history, conflicting transactions, failed reads and exhausted budgets
refuse startup. A recent suffix never counts as complete recovery. The configured
RPCs remain trust dependencies; these consistency checks are not an independent
NoM/execution-client finality or release-epoch attestation.

`/health` returns 503 until recovery completes, and `/status` exposes `ready`,
`recoveryStatus`, `admissionMode: "volatile"` and `admissionDurable: false`.
Recovery has configurable block and time bounds (`--recovery-max-blocks`,
`--recovery-timeout`). Library limits also bound the recovery reader's L1 call,
individual/total response, retained signed-byte, transaction and sender budgets.
Defaults are 65,536 total account blocks, two minutes, 8,192 L1 calls, 2 MiB per
L1 response, 128 MiB total L1 responses, 64 MiB retained transaction bytes,
32,768 transactions and 4,096 senders. Exhaustion requires an explicit operator
decision; startup does not silently increase a limit or admit from partial data.
The ordinary service/ETH RPC paths and workload/RSS limits remain separate R01
qualification requirements.

The Linux process CI lane uses the actual race-instrumented relayer executable,
read-only loopback RPC fixtures and synthetic signed transactions. It checks a
batch outside the old 64-block suffix, a forced process stop/restart, original
hash acknowledgment without another publish, readiness while history is blocked,
incomplete pages, changed frontiers/anchors and budget refusal. These controls do
not establish durable queued admission, power-loss recovery, an authenticated
wallet, live-chain execution or independent operator/maintainer acceptance.
