# Local synchronous admission retention

The default lane remains explicitly volatile. An operator can select a dedicated
private admission store with `--admission-store`, together with checked
`--admission-store-max-records` and `--admission-store-max-raw-bytes` limits.
The parent directory must already exist at its physical path. A new store uses
a private directory; an existing directory must carry the exact version/domain
owner record and database. Unknown, linked, publicly accessible, incomplete or
corrupt store state is refused. There is no implicit adoption, repair or pruning.

In this mode, a successful new or replacement acknowledgement follows a
synchronous atomic local database batch. The exact signed bytes are retained.
Replacement, requeue, inclusion, nonce-used and TTL decisions update their local
lifecycle before the corresponding live maps change. Retired records keep their
original bytes and continue consuming the retention budget. A retention refusal
happens before writing and leaves the original entry active. An ambiguous I/O
result stops this instance and requires a verified reopen; it is not a promise
that no record was committed.

Publication reserves its exact queued objects before external work. Immediately
before a NoM write, the selected records receive a synchronous publication
intent. A lost response or unresolved intent cannot be silently requeued after
restart. Complete source history can bind an exact already-posted transaction
without another publication. A conflicting nonce or unresolved publication holds
readiness. The existing complete-history, canonical-hash-selected nonce,
signature, frontier and atomic-install recovery checks remain mandatory.

`admissionMode: "local-sync"` and `admissionDurable: true` describe this local
storage contract. They do not establish transaction execution, NoM source
authentication, canonicality, finality, accepted production capacity, or hardware
power-loss behavior. A failed or unresolved lane remains unready. Wallets and
clients retain their original signed bytes and transaction hash until they have
independently established the required receipt/finality outcome. They do not
replace or re-sign an uncertain transaction merely because an acknowledgement
or process was lost.

The store binds the complete rollup configuration and ordered posting accounts.
Changing that domain requires an independently reviewed migration; automatic
migration or reuse across domains is refused. The existing pinned database
dependency is used with synchronous writes and strict checks. Record checksums
detect corruption; they are not authentication or consensus proofs.

The defaults retain at most 16,384 active/retired records and 64 MiB of exact raw
signed bytes, with hard ceilings of 262,144 records and 1 GiB of raw bytes. These
are logical retention limits. Database buffers, indexes, journals, compaction,
filesystem allocations and physical disk/RSS require separate operator budgets
and qualification. Retired bytes are never silently removed to free capacity.
The finite full-history response ceiling remains; persisted settled checkpoints
and growing-history recovery remain separate requirements.

The candidate must qualify original and added normal/race controls and actual
race-instrumented CLI acknowledgement/kill/reopen cases. Controlled fixtures do
not establish real wallet integration, independent crash acceptance, supported
platform/hardware behavior, NoM authority/finality or production readiness.
Every original R02 and roadmap acceptance gate remains active.

The dedicated production CLI lane also exercises the actual signed account-block
HTTP publication boundary against an owned loopback endpoint. It distinguishes a
publication absent from source history, an accepted publication whose response is
withheld, and an acknowledged publication. After a kernel kill it reads the real
store, requires an unresolved intent to hold readiness, or recovers the exact
posted hash from complete history without a second publication. Concurrent
replacement is refused while the original publication is outstanding. The
fixture checks the public development account signature and exact carried
transaction bytes; this controlled signature check does not authenticate a live
NoM provider or establish canonicality/finality, wallet or power-loss acceptance.

Recovery never treats a NoM confirmation annotation as proof that an EVM
transaction executed. Every decoded signed transaction is validated, then one
canonical-hash-selected nonce per sender decides whether its exact bytes remain
pending. Already-used nonces are discarded before pending raw-byte and entry
retention budgets or copies. Confirmation metadata still describes a recovered
batch for tracking; it cannot discard an unused nonce at startup. Historical
sender, full-source/RPC/response/decode/time limits remain explicit, and no
persisted settled checkpoint or independent NoM finality proof is introduced.
