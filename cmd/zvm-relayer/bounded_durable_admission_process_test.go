package main

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

func TestBoundedDurableAdmissionCLI(t *testing.T) {
	binary := os.Getenv("ZVM_RELAYER_RECOVERY_BINARY")
	if binary == "" {
		t.Skip("requires the declared actual race-instrumented production CLI resource job")
	}
	if !filepath.IsAbs(binary) || os.Getenv("ZVM_RELAYER_PROFILE_GOMEMLIMIT") != "128MiB" {
		t.Fatal("explicit production binary and child memory profile required")
	}
	events := admissionProfileCheckContainer(t)
	for _, scenario := range []struct{ name, mode, resource string }{
		{"queued_count", "volatile", "transactions"}, {"queued_bytes", "volatile", "raw bytes"},
		{"posted_and_queued_count", "pending", "transactions"}, {"posted_and_queued_bytes", "pending", "raw bytes"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newRecoveryProcessFixture(t, scenario.mode)
			key, err := crypto.GenerateKey()
			if err != nil {
				t.Fatal(err)
			}
			first, firstHash := admissionProfileSignedRaw(t, key, 9)
			second, secondHash := admissionProfileSignedRaw(t, key, 10)
			third, _ := admissionProfileSignedRaw(t, key, 11)
			firstPhase := byte(0)
			expectedPosted := 0
			if scenario.mode == "pending" {
				first, firstHash = bytes.Clone(f.raw), f.hash
				firstPhase = 2
				expectedPosted = 1
			}
			rawBytes := int64(len(first) + len(second))
			maxTransactions, maxRaw := "2", strconv.FormatInt(int64(64<<20), 10)
			smaller := []string{"-max-transactions", "1"}
			if scenario.resource == "raw bytes" {
				maxTransactions, maxRaw = "16", strconv.FormatInt(rawBytes, 10)
				smaller = []string{"-max-raw-bytes", strconv.Itoa(len(first))}
			}
			directory := filepath.Join(t.TempDir(), "admissions")
			flags := []string{"-admission-store", directory, "-max-transactions", maxTransactions, "-max-raw-bytes", maxRaw,
				"-max-senders", "4", "-max-sender-transactions", "16", "-max-sender-raw-bytes", "1048576", "-max-admission", "4"}
			client := &http.Client{Timeout: 2 * time.Second}
			var maximumRSS uint64
			for attempt := 0; attempt < 2; attempt++ {
				p := f.startWithRecoveryFlags(binary, "65536", flags...)
				recoveryHTTPStatus(t, p.url+"/health", 200)
				status := recoveryProcessSnapshot(t, p)
				resource, err := admissionProfileStatusAt(client, p.url)
				if err != nil || !status.Ready || !status.AdmissionDurable || status.AdmissionMode != "local-sync" || status.PostedPending != expectedPosted {
					t.Fatal("combined durable profile did not start", err)
				}
				if attempt == 1 && (resource.RawBytes != rawBytes || resource.Queued+resource.Posted != 2) {
					t.Fatal("restart lost or double-counted active reservations")
				}
				if recoveryProcessSubmit(t, p, first) != firstHash || recoveryProcessSubmit(t, p, second) != secondHash {
					t.Fatal("combined profile changed original acknowledged hashes")
				}
				code, reply, err := admissionProfileCall(client, p.url, third)
				if err != nil || code != 200 || reply == nil || reply.Error == nil || reply.Error.Code != -32005 {
					t.Fatal("full durable profile lost typed capacity refusal", err)
				}
				after, err := admissionProfileStatusAt(client, p.url)
				if err != nil || !after.Ready || after.RawBytes != rawBytes || after.Queued+after.Posted != 2 {
					t.Fatal("refused work changed active reservations", err)
				}
				rss, high, err := admissionProfileRSS(p.command.Process.Pid)
				if err != nil || rss > 384<<20 || high > 384<<20 {
					t.Fatal("combined profile exceeded its declared child RSS ceiling", err)
				}
				maximumRSS = max(maximumRSS, rss, high)
				p.kill(t)
				durableCLIStoredWire(t, directory, firstHash, first, firstPhase)
				durableCLIStoredWire(t, directory, secondHash, second, 0)
				if attempt == 0 {
					reduced := append(append([]string{}, flags...), smaller...)
					refused := f.startWithRecoveryFlags(binary, "65536", reduced...)
					if refused.wait(t) == nil {
						t.Fatal("smaller profile silently omitted previously acknowledged active work")
					}
					durableCLIStoredWire(t, directory, firstHash, first, firstPhase)
					durableCLIStoredWire(t, directory, secondHash, second, 0)
				}
			}
			if f.writes.Load() != 0 || f.pages.Load() != 9 {
				t.Fatal("combined capacity recovery skipped complete source history or published")
			}
			t.Logf(`BOUNDED-DURABLE-ADMISSION-CLI {"case":%q,"resource":%q,"process_lifetimes":3,"forced_stops":2,"processes_joined":3,"complete_account_blocks":130,"account_page_reads":9,"smaller_profile_refused":true,"original_hashes_and_stored_signed_bytes_equal":true,"active_reservations":2,"active_raw_bytes":%d,"max_observed_child_RSS_bytes":%d,"write_RPCs":0}`, scenario.name, scenario.resource, rawBytes, maximumRSS)
		})
	}
	admissionProfileCheckNoOOM(t, events)
}
