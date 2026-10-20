package main

// These are unexported helpers in the monorepo's cmd/zvm, shared by the node,
// the relayer and the bridge. They live in zvm-core now (service for the two
// lifecycle helpers, cli for the rest); the aliases below keep the copied
// command files verbatim, so they can be re-synced from cmd/zvm without edits.

import (
	"context"
	"flag"
	"time"

	"github.com/Zenoglyphs-VM/zvm-core/cli"
	"github.com/Zenoglyphs-VM/zvm-core/service"
)

func signalContext() (context.Context, context.CancelFunc) { return service.SignalContext() }

func waitFor[T any](ctx context.Context, what string, fn func() (T, error)) (T, error) {
	return service.WaitFor(ctx, what, fn)
}

// clientFlags keeps the monorepo's unexported field names so that a command
// file copied from cmd/zvm compiles here unchanged.
type clientFlags struct {
	config *string
	l1     *string
	eth    *string
	index  *uint
	wait   *time.Duration
}

func addClientFlags(fs *flag.FlagSet) clientFlags {
	f := cli.AddClientFlags(fs)
	return clientFlags{config: f.Config, l1: f.L1, eth: f.Eth, index: f.Index, wait: f.Wait}
}

// run.go carries its own writeJSON, so it is not aliased here.

var (
	parseEVMOrNoM = cli.ParseEVMOrNoM
	parseHexData  = cli.ParseHexData
	parseUnits    = cli.ParseUnits
	formatUnits   = cli.FormatUnits
	parseFeeToken = cli.ParseFeeToken
	waitReceipt   = cli.WaitReceipt
)
