// Command zvm-relayer is the batch lane: it takes signed EVM transactions over
// JSON-RPC, packs them into batches and posts each batch to NoM as an account
// block, from a rotating set of relayer accounts.
//
// run.go is a verbatim copy of cmd/zvm/relayer.go in the zvm deployment repo,
// which is still the source of truth. The register/dispatch harness below is
// the same one cmd/zvm uses, so that file can be re-copied without edits.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
)

type command struct {
	summary string
	run     func(args []string) error
}

var commands = map[string]command{}

func register(name, summary string, run func(args []string) error) {
	commands[name] = command{summary: summary, run: run}
}

// defaultCommand runs when no subcommand is named, so that the container
// entrypoint is just `zvm-relayer -l1 ... -eth ...`.
const defaultCommand = "relayer"

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	name, args := defaultCommand, os.Args[1:]
	if len(args) > 0 {
		if _, ok := commands[args[0]]; ok {
			name, args = args[0], args[1:]
		} else if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
			usage()
			os.Exit(2)
		}
	}
	cmd := commands[name]
	if err := cmd.run(args); err != nil {
		fmt.Fprintf(os.Stderr, "zvm-relayer %s: %v\n", name, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: zvm-relayer [command] [flags]   (default: %s)\n\n", defaultCommand)
	names := make([]string, 0, len(commands))
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(os.Stderr, "  %-14s %s\n", n, commands[n].summary)
	}
}
