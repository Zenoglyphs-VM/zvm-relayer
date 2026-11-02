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

`/health` returns 503 until recovery completes, and `/status` exposes `ready`
and `recoveryStatus`. The default mode reports `admissionMode: "volatile"` and
`admissionDurable: false`. Selecting `--admission-store` enables optional local
synchronous retention; see [the local storage contract](docs/DURABLE_ADMISSION.md)
for lifecycle, limits, uncertainty and migration boundaries.
Every recovery-reader budget is operator-configurable: block count, timeout,
L1 calls, individual and total response bytes, retained signed bytes,
transactions and senders. See the startup budget table below.
Defaults are 65,536 total account blocks, two minutes, 8,192 L1 calls, 2 MiB per
L1 response, 128 MiB total L1 responses, 64 MiB retained transaction bytes,
32,768 transactions and 4,096 senders. Exhaustion requires an explicit operator
decision; startup does not silently increase a limit or admit from partial data.
The ordinary service/ETH RPC paths and workload/RSS limits remain separate R01
qualification requirements.

The Linux process CI lane uses the actual race-instrumented relayer executable
and controlled loopback RPC fixtures. It checks complete history, readiness and
budgets, volatile or local-sync acknowledgement, exclusive storage ownership,
original-hash kernel-kill/reopen, and signed-publication uncertainty. The original
zero-write fixtures remain read-only; the publication controls write only to an
owned loopback endpoint using a public development account. These controls do not
establish live-chain authority/finality, an authenticated production wallet,
hardware power-loss behavior or independent operator/maintainer acceptance.

A NoM confirmation annotation does not prove EVM execution. Recovery validates
every decoded signed transaction and reads one cached nonce per sender at the
frozen canonical execution hash. Unused nonces keep their exact bytes regardless
of that annotation. Already-used nonces are discarded before pending raw-byte
and entry retention budgets or copies. Historical senders still consume the
checked sender budget; all source pages, response bytes, decoding and elapsed
work remain bounded. Malformed historical batches remain refused rather than
being hidden by a confirmation annotation. This introduces no persisted settled
checkpoint, unbounded-history recovery or measured RSS/elapsed improvement.

### Selecting startup recovery budgets

All recovery-reader budgets are available on the CLI and validated before the
rollup file, account material or RPC connections are opened. Library callers can
use `RecoveryLimits.Checked()` to obtain the same effective values. Zero selects
the corresponding default; it never disables a bound. Negative values and
out-of-range values refuse startup.

| Flag | Default | Maximum |
|---|---:|---:|
| `--recovery-max-blocks` | 65,536 | 16,777,216 |
| `--recovery-max-calls` | 8,192 | 1,048,576 |
| `--recovery-max-response-bytes` | 2,097,152 | 16,777,216 |
| `--recovery-max-total-response-bytes` | 134,217,728 | 1,073,741,824 |
| `--recovery-max-raw-bytes` | 67,108,864 | 536,870,912 |
| `--recovery-max-transactions` | 32,768 | 1,048,576 |
| `--recovery-max-senders` | 4,096 | 1,048,576 |
| `--recovery-timeout` | 2m | 30m |

The single-response minimum is 1,024 bytes; the total response budget must be at
least the effective single-response budget. The timeout minimum is 1ms. Every
other nonzero budget must be positive. Byte values are exact bytes, not MiB.

Complete account pages contain block Data even when execution has settled a
batch. JSON encoding and base64 data also consume response bytes, so increasing
the block count alone does not guarantee that a history fits. Select all budgets
explicitly for the observed account history and available operator resources.
For example, `--recovery-max-total-response-bytes 268435456 --recovery-timeout 3m`
selects a 256 MiB cumulative L1 response budget while keeping the 2 MiB individual
response cap and the other default bounds.

The reader distinguishes `ErrRecoveryResponseBytes` from
`ErrRecoveryTotalResponseBytes` with `errors.Is`. Both errors have fixed public
messages and retain no endpoint, remote diagnostic or signed payload. A fully
exhausted total budget refuses before another transport call. An unknown-length
body may be read up to the applicable cap plus one byte solely to detect an
overrun. These are decoded HTTP-body/admission budgets, not network, OS-memory or
whole-service resource quotas.

Budget refusal preserves the original signed identity and blocks readiness;
there is no automatic increase, suffix scan or retry. Explicitly increasing a
budget does not establish an accepted production capacity. The finite 1 GiB hard
ceiling still limits a growing account history. Persisted settled checkpoints,
independent source authentication/finality, wallet exact-byte recovery and
operator/maintainer acceptance remain separate requirements. Admission defaults to
volatile; an explicit local-sync store retains its separate durability and
retired-record limits. Startup history limits do not replace journal or active
admission capacity checks.


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

### Combined durable and active admission budgets

Active queued and posted work shares the same count and signed-byte limits
in both modes. Full-history recovery and queued journal recovery consume one
combined active budget; an exact hash present in both sources counts once.
An active-capacity refusal precedes journal lifecycle writes and live map
installation. Unresolved publication remains a readiness hold, never a blind
resend. A smaller startup profile can refuse previously accepted work; select
an explicit fitting profile rather than deleting records or silently omitting
transactions. Nonce-gap limits govern new submissions; restoration still keeps
all previously acknowledged unused nonces subject to the active count, sender
and byte budgets.

Journal retention limits independently include retired original signed bytes.
Retiring, posting or settling an entry does not prune that audit history or
relax its finite retained-record/raw-byte capacity. Logical active admission
limits, physical database size, RSS and accepted operator capacity remain
different measurements. The combined source requires fresh native/race and
actual executable qualification; neither standalone Draft qualifies it.
