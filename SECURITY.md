# Security policy

Report a vulnerability privately, through this repository's **Security** tab,
**Report a vulnerability**. Please don't open a public issue or pull request
for it.

Say what you found, how to reproduce it, and what it lets someone do. The
reply comes in the same private advisory.

## Scope

This code runs on ZVM devnets, such as zvm-devnet (chain 7340469). Devnet
tokens have no value, and there is no bounty. Keys derived from go-zenon's
committed devnet mnemonic, or as `keccak256("zvm-dev-evm-<i>")`, are public by
design and are not a finding.
