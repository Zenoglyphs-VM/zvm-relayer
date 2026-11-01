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


## Bounded admission profile

Queued and posted transactions retain global and per-sender count/byte
reservations until settlement or explicit queue expiry. Moving work into a
posted batch does not free capacity. A refused replacement preserves the
original signed bytes, hash and reservation. Recovery installs no partial state
when its reconstructed queue would exceed the same admission limits.

Finite defaults are 16,384 transactions, 64 MiB raw signed bytes, 4,096 senders,
1,024 transactions and 4 MiB signed bytes per sender, a future nonce gap of
4,096, 32 concurrent admissions and a five-second admission deadline. Zero
selects the documented finite default. Configure limits before starting the
service using the `--max-*` and `--admission-timeout` flags. Pending nonce calls
share the admission slots. Head-account cache growth is also sender-bounded.

HTTP POST capacity is reserved before reading a body. Health/status GETs remain
available during congestion. The body cap is 8 MiB, a JSON-RPC batch contains
1..100 requests, and the decoder stops before allocating the 101st request
object. `--request-timeout` bounds the complete request, including a stalled
socket body; its default is 15 seconds and its maximum is one minute. Capacity
errors retain RPC code -32005; private upstream transport details are hidden.
Historical nonce selectors retain the separately reviewed state/canonicality
semantics. These limits do not turn volatile acknowledgment into durability or
grant authority to sign, replace, change nonce/context or automatically resend.

The proposed Linux resource job pins its Go image by digest and requires actual
cgroup limits of 2 GiB memory, two CPUs and 128 tasks. It builds the production
race-instrumented executable once for all recovery and admission scenarios.
Three gapped bursts use 768 concurrent attempts plus 288 fresh-nonce fill
requests, a 64 KiB retained-byte profile, 16 senders and four request/admission
slots. Separate socket/posted-work checks verify readiness, typed refusal and
slot release. The child has a 128 MiB Go soft memory target and a measured
384 MiB RSS ceiling. Missing cgroup/RSS counters or any OOM event fail the lane.

A separate 128-cycle component workload checks logical resource release and
GC-assisted heap liveness against a warmed baseline while retaining its fixture
input bytes. That GC-assisted result is not a production OS-RSS reclamation
proof. The actual process profile reports kernel RSS independently. Operator
limit selection, broader/default-scale workload/RSS, platform, power-loss,
real-wallet/provider, NoM/finality and maintainer acceptance remain separate.
