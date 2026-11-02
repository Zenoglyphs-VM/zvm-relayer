package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Inspect the independently specified version-one binary record after joining
// the production process. This oracle reads the actual retained signed bytes.
func durableCLIStoredWire(t *testing.T, directory string, hash common.Hash, expected []byte, phase byte) {
	t.Helper()
	owner, err := os.ReadFile(filepath.Join(directory, "owner-v1"))
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(owner, []byte("\n"))
	if len(lines) != 3 || string(lines[0]) != "ZVM admission owner v1" {
		t.Fatal("store owner record refused")
	}
	domain, err := hex.DecodeString(string(lines[1]))
	if err != nil || len(domain) != sha256.Size {
		t.Fatal("store domain record refused")
	}
	db, err := leveldb.OpenFile(filepath.Join(directory, "data"), &opt.Options{ReadOnly: true, Strict: opt.StrictAll, ErrorIfMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value, err := db.Get(append([]byte("t/"), hash[:]...), &opt.ReadOptions{Strict: opt.StrictAll})
	if err != nil || len(value) < 37+sha256.Size || string(value[:8]) != "ZVMADM01" {
		t.Fatal("actual persisted admission record unavailable")
	}
	length := int(binary.BigEndian.Uint32(value[8:12]))
	if length != len(expected) || len(value) != 37+length+sha256.Size || value[34] != 0 || value[35] != 0 || value[36] != phase || !bytes.Equal(value[37:37+length], expected) {
		t.Fatal("actual store changed original signed bytes or lifecycle phase")
	}
	h := sha256.New()
	h.Write(domain)
	h.Write(hash[:])
	h.Write(value[:len(value)-sha256.Size])
	if !bytes.Equal(h.Sum(nil), value[len(value)-sha256.Size:]) {
		t.Fatal("actual retained record checksum refused")
	}
}

func TestDurableAdmissionCLIProcesses(t *testing.T) {
	binary := os.Getenv("ZVM_RELAYER_RECOVERY_BINARY")
	if binary == "" {
		t.Skip("requires the declared actual race-instrumented production CLI job")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("production CLI binary must be explicit")
	}
	t.Run("queued_ACK_survives_kernel_kill_and_exact_reopen", func(t *testing.T) {
		f := newRecoveryProcessFixture(t, "volatile")
		original := bytes.Clone(f.raw)
		directory := filepath.Join(t.TempDir(), "admissions")
		for attempt := 0; attempt < 2; attempt++ {
			p := f.startWithRecoveryFlags(binary, "65536", "-admission-store", directory)
			recoveryHTTPStatus(t, p.url+"/health", 200)
			status := recoveryProcessSnapshot(t, p)
			if !status.Ready || status.AdmissionMode != "local-sync" || !status.AdmissionDurable || status.PostedPending != 0 || status.Queued != attempt {
				t.Fatal("durable queue/status did not reopen exactly")
			}
			if recoveryProcessSubmit(t, p, original) != f.hash || recoveryProcessSnapshot(t, p).Queued != 1 {
				t.Fatal("durable original hash not acknowledged")
			}
			p.kill(t)
			durableCLIStoredWire(t, directory, f.hash, original, 0)
		}
		if !bytes.Equal(f.raw, original) || f.writes.Load() != 0 || f.pages.Load() != 6 {
			t.Fatal("durable restart changed bytes, skipped complete history or published")
		}
		t.Log(`DURABLE-ADMISSION-CLI {"case":"queued_ack_kernel_kill","process_lifetimes":2,"forced_stops":2,"processes_joined":2,"complete_account_blocks":130,"original_hash_acknowledged_twice":true,"actual_retained_signed_bytes_equal":true,"queue_restored":true,"write_RPCs":0}`)
	})
	t.Run("posted_identity_is_recovered_without_another_publication", func(t *testing.T) {
		f := newRecoveryProcessFixture(t, "pending")
		original := bytes.Clone(f.raw)
		directory := filepath.Join(t.TempDir(), "admissions")
		for attempt := 0; attempt < 2; attempt++ {
			p := f.startWithRecoveryFlags(binary, "65536", "-admission-store", directory)
			recoveryHTTPStatus(t, p.url+"/health", 200)
			status := recoveryProcessSnapshot(t, p)
			if !status.Ready || status.AdmissionMode != "local-sync" || !status.AdmissionDurable || status.PostedPending != 1 || status.Queued != 0 || recoveryProcessSubmit(t, p, original) != f.hash {
				t.Fatal("durable posted identity changed")
			}
			p.kill(t)
			durableCLIStoredWire(t, directory, f.hash, original, 2)
		}
		if !bytes.Equal(f.raw, original) || f.writes.Load() != 0 || f.pages.Load() != 6 {
			t.Fatal("posted store recovery skipped history or published again")
		}
		t.Log(`DURABLE-ADMISSION-CLI {"case":"posted_hash_kernel_kill","process_lifetimes":2,"forced_stops":2,"processes_joined":2,"complete_account_blocks":130,"batch_height":2,"original_hash_acknowledged_twice":true,"actual_retained_signed_bytes_equal":true,"posted_restored":true,"write_RPCs":0}`)
	})
	t.Run("second_production_writer_is_refused", func(t *testing.T) {
		f := newRecoveryProcessFixture(t, "volatile")
		directory := filepath.Join(t.TempDir(), "admissions")
		first := f.startWithRecoveryFlags(binary, "65536", "-admission-store", directory)
		recoveryHTTPStatus(t, first.url+"/health", 200)
		if recoveryProcessSubmit(t, first, f.raw) != f.hash {
			t.Fatal("first writer did not acknowledge")
		}
		second := f.startWithRecoveryFlags(binary, "65536", "-admission-store", directory)
		if second.wait(t) == nil {
			t.Fatal("second store writer started")
		}
		if recoveryProcessSubmit(t, first, f.raw) != f.hash || recoveryProcessSnapshot(t, first).Queued != 1 {
			t.Fatal("writer refusal changed active admission")
		}
		first.kill(t)
		durableCLIStoredWire(t, directory, f.hash, f.raw, 0)
		if f.writes.Load() != 0 {
			t.Fatal("writer exclusion published")
		}
		t.Log(`DURABLE-ADMISSION-CLI {"case":"exclusive_production_writer","process_lifetimes":2,"forced_stops":1,"processes_joined":2,"second_writer_refused":true,"original_hash_retained":true,"actual_retained_signed_bytes_equal":true,"write_RPCs":0}`)
	})
}
