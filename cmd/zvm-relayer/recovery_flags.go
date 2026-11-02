package main

import (
	"flag"
	"github.com/Zenoglyphs-VM/zvm-relayer/relayer"
)

// Register every library recovery bound from its checked defaults. The caller
// checks the parsed values before opening configuration, credentials or RPCs.
func recoveryFlags(fs *flag.FlagSet) (*relayer.RecoveryLimits, error) {
	limits, err := (relayer.RecoveryLimits{}).Checked()
	if err != nil {
		return nil, err
	}
	fs.Uint64Var(&limits.MaxBlocks, "recovery-max-blocks", limits.MaxBlocks, "maximum complete startup account-history blocks across all accounts")
	fs.IntVar(&limits.MaxCalls, "recovery-max-calls", limits.MaxCalls, "maximum startup recovery L1 RPC calls")
	fs.Int64Var(&limits.MaxResponseBytes, "recovery-max-response-bytes", limits.MaxResponseBytes, "maximum bytes in one startup recovery L1 JSON response")
	fs.Int64Var(&limits.MaxTotalResponseBytes, "recovery-max-total-response-bytes", limits.MaxTotalResponseBytes, "maximum cumulative startup recovery L1 JSON response bytes")
	fs.Int64Var(&limits.MaxRawBytes, "recovery-max-raw-bytes", limits.MaxRawBytes, "maximum retained signed transaction bytes during startup recovery")
	fs.IntVar(&limits.MaxTransactions, "recovery-max-transactions", limits.MaxTransactions, "maximum retained startup recovery transactions")
	fs.IntVar(&limits.MaxSenders, "recovery-max-senders", limits.MaxSenders, "maximum distinct startup recovery transaction senders")
	fs.DurationVar(&limits.Timeout, "recovery-timeout", limits.Timeout, "maximum complete startup recovery duration")
	return &limits, nil
}
