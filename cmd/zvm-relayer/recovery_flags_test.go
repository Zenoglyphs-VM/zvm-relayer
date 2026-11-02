package main

import (
	"flag"
	"github.com/Zenoglyphs-VM/zvm-relayer/relayer"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRecoveryOperatorFlags(t *testing.T) {
	parse := func(args ...string) relayer.RecoveryLimits {
		t.Helper()
		fs := flag.NewFlagSet("recovery-controls", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		limits, err := recoveryFlags(fs)
		if err != nil {
			t.Fatal(err)
		}
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		checked, err := limits.Checked()
		if err != nil {
			t.Fatal(err)
		}
		return checked
	}
	defaults, err := (relayer.RecoveryLimits{}).Checked()
	if err != nil {
		t.Fatal(err)
	}
	if parse() != defaults {
		t.Fatal("CLI and library default budgets differ")
	}
	got := parse("-recovery-max-blocks", "131072", "-recovery-max-calls", "16384",
		"-recovery-max-response-bytes", "4194304", "-recovery-max-total-response-bytes", "268435456",
		"-recovery-max-raw-bytes", "134217728", "-recovery-max-transactions", "65536",
		"-recovery-max-senders", "8192", "-recovery-timeout", "3m")
	want := relayer.RecoveryLimits{MaxBlocks: 131072, MaxCalls: 16384, MaxResponseBytes: 4 << 20,
		MaxTotalResponseBytes: 256 << 20, MaxRawBytes: 128 << 20, MaxTransactions: 65536, MaxSenders: 8192, Timeout: 3 * time.Minute}
	if got != want {
		t.Fatal("an operator flag did not reach its effective library budget")
	}
	if parse("-recovery-max-total-response-bytes", "0") != defaults {
		t.Fatal("zero must preserve the documented library default, never mean unlimited")
	}
}

func TestInvalidRecoveryBudgetPrecedesConfigAndNetwork(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "must-not-open-rollup.json")
	cases := []struct{ name, flag, value string }{
		{"blocks", "-recovery-max-blocks", "16777217"},
		{"calls-negative", "-recovery-max-calls", "-1"}, {"calls-large", "-recovery-max-calls", "1048577"},
		{"response-small", "-recovery-max-response-bytes", "1023"}, {"response-large", "-recovery-max-response-bytes", "16777217"},
		{"total-below-response", "-recovery-max-total-response-bytes", "1048576"}, {"total-large", "-recovery-max-total-response-bytes", "1073741825"},
		{"raw-negative", "-recovery-max-raw-bytes", "-1"}, {"raw-large", "-recovery-max-raw-bytes", "536870913"},
		{"transactions-negative", "-recovery-max-transactions", "-1"}, {"transactions-large", "-recovery-max-transactions", "1048577"},
		{"senders-negative", "-recovery-max-senders", "-1"}, {"senders-large", "-recovery-max-senders", "1048577"},
		{"timeout-small", "-recovery-timeout", "1us"}, {"timeout-large", "-recovery-timeout", "31m"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := runRelayer([]string{"-config", missing, "-l1", "invalid-no-network", "-eth", "invalid-no-network", tc.flag, tc.value})
			if err == nil || err.Error() != "invalid recovery limits" || strings.Contains(err.Error(), missing) {
				t.Fatalf("invalid budget reached configuration or network: %v", err)
			}
		})
	}
}
