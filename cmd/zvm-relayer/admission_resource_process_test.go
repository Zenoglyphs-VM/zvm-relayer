package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zenoglyphs-VM/zvm-relayer/relayer"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type admissionProfileStatus struct {
	Ready    bool           `json:"ready"`
	Queued   int            `json:"queued"`
	Posted   int            `json:"postedPending"`
	Senders  int            `json:"senders"`
	RawBytes int64          `json:"retainedRawBytes"`
	Limits   relayer.Limits `json:"limits"`
}

func admissionProfileStatusAt(client *http.Client, url string) (admissionProfileStatus, error) {
	var status admissionProfileStatus
	response, err := client.Get(url + "/status")
	if err != nil {
		return status, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return status, fmt.Errorf("status HTTP %d", response.StatusCode)
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&status); err != nil {
		return status, err
	}
	return status, nil
}

func admissionProfileFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil || len(raw) > 64<<10 {
		return nil, fmt.Errorf("profile counter incomplete or oversized")
	}
	return raw, nil
}

func admissionProfileCounter(path string) (uint64, error) {
	raw, err := admissionProfileFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
}

func admissionProfileEvents() (map[string]uint64, error) {
	raw, err := admissionProfileFile("/sys/fs/cgroup/memory.events")
	if err != nil {
		return nil, err
	}
	out := map[string]uint64{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("malformed cgroup event")
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}
		out[fields[0]] = value
	}
	if _, ok := out["oom"]; !ok {
		return nil, fmt.Errorf("missing cgroup OOM counter")
	}
	if _, ok := out["oom_kill"]; !ok {
		return nil, fmt.Errorf("missing cgroup OOM kill counter")
	}
	return out, nil
}

func admissionProfileRSS(pid int) (uint64, uint64, error) {
	raw, err := admissionProfileFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
	if err != nil {
		return 0, 0, err
	}
	values := map[string]uint64{}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || (fields[0] != "VmRSS:" && fields[0] != "VmHWM:") {
			continue
		}
		if fields[2] != "kB" {
			return 0, 0, fmt.Errorf("unknown RSS units")
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || value > ^uint64(0)/1024 {
			return 0, 0, fmt.Errorf("invalid RSS counter")
		}
		values[fields[0]] = value * 1024
	}
	if values["VmRSS:"] == 0 || values["VmHWM:"] == 0 {
		return 0, 0, fmt.Errorf("missing RSS counters")
	}
	return values["VmRSS:"], values["VmHWM:"], nil
}

func admissionProfileCheckContainer(t *testing.T) map[string]uint64 {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Fatal("the declared resource profile requires Linux amd64")
	}
	memory, err := admissionProfileCounter("/sys/fs/cgroup/memory.max")
	if err != nil || memory != 2<<30 {
		t.Fatalf("the declared 2 GiB hard memory limit is not established: %v", err)
	}
	pids, err := admissionProfileCounter("/sys/fs/cgroup/pids.max")
	if err != nil || pids != 128 {
		t.Fatalf("the declared PID limit is not established: %v", err)
	}
	cpu, err := admissionProfileFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(cpu))
	if len(fields) != 2 {
		t.Fatal("CPU limit is not established")
	}
	quota, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	period, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil || period == 0 || quota/period != 2 || quota%period != 0 {
		t.Fatal("the declared two-CPU hard limit is not established")
	}
	before, err := admissionProfileEvents()
	if err != nil || before["oom"] != 0 || before["oom_kill"] != 0 {
		t.Fatalf("container already encountered an OOM event: %v", err)
	}
	return before
}

func admissionProfileCheckNoOOM(t *testing.T, before map[string]uint64) {
	t.Helper()
	after, err := admissionProfileEvents()
	if err != nil || after["oom"] != before["oom"] || after["oom_kill"] != before["oom_kill"] {
		t.Fatalf("resource profile encountered an OOM event: %v", err)
	}
}

func admissionProfileSignedRaw(t *testing.T, key *ecdsa.PrivateKey, nonce uint64) ([]byte, common.Hash) {
	t.Helper()
	to := common.Address{7}
	tx, err := types.SignNewTx(key, types.LatestSignerForChainID(big.NewInt(recoveryProcessChain)), &types.DynamicFeeTx{ChainID: big.NewInt(recoveryProcessChain), Nonce: nonce, Gas: 50000, GasFeeCap: big.NewInt(2), GasTipCap: big.NewInt(1), To: &to, Value: big.NewInt(1), Data: make([]byte, 1024)})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return raw, tx.Hash()
}

func admissionProfileRaw(t *testing.T, nonce uint64) ([]byte, common.Hash) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return admissionProfileSignedRaw(t, key, nonce)
}

func admissionProfileCall(client *http.Client, url string, raw []byte) (int, *rpcResponse, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction", "params": []string{hexutil.Encode(raw)}})
	if err != nil {
		return 0, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 8192))
		return response.StatusCode, nil, err
	}
	var reply rpcResponse
	if err = json.NewDecoder(io.LimitReader(response.Body, 8192)).Decode(&reply); err != nil {
		return 0, nil, err
	}
	return response.StatusCode, &reply, nil
}

func TestAdmissionResourceCLI(t *testing.T) {
	binary := os.Getenv("ZVM_RELAYER_RECOVERY_BINARY")
	if binary == "" {
		t.Skip("requires the declared actual production CLI resource job")
	}
	if !filepath.IsAbs(binary) || os.Getenv("ZVM_RELAYER_PROFILE_GOMEMLIMIT") != "128MiB" {
		t.Fatal("explicit production binary and child memory profile are required")
	}
	before := admissionProfileCheckContainer(t)
	t.Run("bounded_gapped_bursts_and_idle_return", func(t *testing.T) {
		f := newRecoveryProcessFixture(t, "volatile")
		p := f.start(binary, "65536", "-max-transactions", "96", "-max-raw-bytes", "65536", "-max-senders", "16", "-max-sender-transactions", "8", "-max-sender-raw-bytes", "8192", "-max-nonce-gap", "128", "-max-admission", "4", "-admission-timeout", "200ms", "-request-timeout", "500ms", "-queue-ttl", "2s")
		recoveryHTTPStatus(t, p.url+"/health", 200)
		transport := &http.Transport{MaxIdleConnsPerHost: 32, MaxConnsPerHost: 32}
		defer transport.CloseIdleConnections()
		client := &http.Client{Timeout: 2 * time.Second, Transport: transport}
		baseline, _, err := admissionProfileRSS(p.command.Process.Pid)
		if err != nil {
			t.Fatal(err)
		}
		stop, sampled := make(chan struct{}), make(chan struct{})
		var stopOnce sync.Once
		var sampleError atomic.Bool
		var maximumRSS, maximumRaw, maximumQueued, maximumSenders atomic.Uint64
		go func() {
			defer close(sampled)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					status, err := admissionProfileStatusAt(client, p.url)
					if err != nil {
						sampleError.Store(true)
						continue
					}
					rss, high, err := admissionProfileRSS(p.command.Process.Pid)
					if err != nil {
						sampleError.Store(true)
						continue
					}
					maximumRSS.Store(max(maximumRSS.Load(), rss, high))
					maximumRaw.Store(max(maximumRaw.Load(), uint64(max(status.RawBytes, 0))))
					maximumQueued.Store(max(maximumQueued.Load(), uint64(max(status.Queued, 0))))
					maximumSenders.Store(max(maximumSenders.Load(), uint64(max(status.Senders, 0))))
					if !status.Ready || status.RawBytes < 0 || status.RawBytes > 65536 || status.Queued > 96 || status.Senders > 16 || status.Posted != 0 {
						sampleError.Store(true)
					}
				}
			}
		}()
		stopSampling := func() {
			stopOnce.Do(func() { close(stop) })
			select {
			case <-sampled:
			case <-time.After(3 * time.Second):
				t.Error("resource sampler did not join")
			}
		}
		t.Cleanup(stopSampling)
		var accepted, busy, capacity, failed atomic.Uint64
		for wave := 0; wave < 3; wave++ {
			inputs := make([][]byte, 256)
			hashes := make([]common.Hash, 256)
			keys := make([]*ecdsa.PrivateKey, 16)
			for i := range keys {
				key, err := crypto.GenerateKey()
				if err != nil {
					t.Fatal(err)
				}
				keys[i] = key
			}
			for i := range inputs {
				inputs[i], hashes[i] = admissionProfileSignedRaw(t, keys[i%16], uint64(1+i/16))
			}
			var cursor atomic.Int32
			var group sync.WaitGroup
			for worker := 0; worker < 16; worker++ {
				group.Add(1)
				go func() {
					defer group.Done()
					for {
						index := int(cursor.Add(1) - 1)
						if index >= len(inputs) {
							return
						}
						code, reply, err := admissionProfileCall(client, p.url, inputs[index])
						if err != nil {
							failed.Add(1)
							continue
						}
						if code == 503 {
							busy.Add(1)
							continue
						}
						if code != 200 || reply == nil {
							failed.Add(1)
							continue
						}
						if reply.Error != nil {
							if reply.Error.Code == -32005 {
								capacity.Add(1)
							} else {
								failed.Add(1)
							}
							continue
						}
						encoded, _ := json.Marshal(reply.Result)
						var actual common.Hash
						if json.Unmarshal(encoded, &actual) != nil || actual != hashes[index] {
							failed.Add(1)
						} else {
							accepted.Add(1)
						}
					}
				}()
			}
			group.Wait()
			// Fresh nonces fill the same declared sender cohort after early
			// HTTP backpressure. No ambiguous original request is resent.
			for i := 0; i < 96; i++ {
				raw, want := admissionProfileSignedRaw(t, keys[i%16], uint64(65+i/16))
				code, reply, err := admissionProfileCall(client, p.url, raw)
				if err != nil {
					failed.Add(1)
				} else if code == 503 {
					busy.Add(1)
				} else if code != 200 || reply == nil {
					failed.Add(1)
				} else if reply.Error != nil {
					if reply.Error.Code == -32005 {
						capacity.Add(1)
					} else {
						failed.Add(1)
					}
				} else {
					encoded, _ := json.Marshal(reply.Result)
					var actual common.Hash
					if json.Unmarshal(encoded, &actual) != nil || actual != want {
						failed.Add(1)
					} else {
						accepted.Add(1)
					}
				}
			}
			deadline := time.Now().Add(5 * time.Second)
			empty := false
			for time.Now().Before(deadline) {
				status, err := admissionProfileStatusAt(client, p.url)
				if err == nil && status.Queued == 0 && status.Posted == 0 && status.RawBytes == 0 && status.Senders == 0 {
					empty = true
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !empty {
				sampleError.Store(true)
			}
		}
		stopSampling()
		_, high, err := admissionProfileRSS(p.command.Process.Pid)
		if err != nil {
			t.Fatal(err)
		}
		peak := max(maximumRSS.Load(), high)
		if failed.Load() != 0 || sampleError.Load() || accepted.Load() == 0 || busy.Load() == 0 || capacity.Load() == 0 || maximumRaw.Load() < 32<<10 || peak > 384<<20 || f.writes.Load() != 0 {
			t.Fatalf("resource workload failed its declared limits: accepted=%d busy=%d capacity=%d failures=%d sampling_error=%v", accepted.Load(), busy.Load(), capacity.Load(), failed.Load(), sampleError.Load())
		}
		p.kill(t)
		record := map[string]any{"case": "actual_CLI_bounded_gapped_bursts", "waves": 3, "burst_requests": 768, "fresh_nonce_fill_requests": 288, "accepted_original_hashes": accepted.Load(), "HTTP_backpressure": busy.Load(), "typed_capacity_refusals": capacity.Load(),
			"baseline_RSS_bytes": baseline, "kernel_peak_RSS_bytes": peak, "declared_RSS_ceiling_bytes": 384 << 20, "observed_peak_retained_bytes": maximumRaw.Load(), "observed_peak_queued": maximumQueued.Load(), "observed_peak_senders": maximumSenders.Load(),
			"final_queued": 0, "final_posted": 0, "final_retained_bytes": 0, "final_senders": 0, "sampler_joined": true, "process_joined": true, "write_RPCs": 0, "OS_RSS_baseline_reclamation_claimed": false}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(string(raw))
	})
	t.Run("slow_socket_releases_slot_and_posted_work_keeps_budget", func(t *testing.T) {
		f := newRecoveryProcessFixture(t, "pending")
		p := f.start(binary, "65536", "-max-transactions", "1", "-max-admission", "1", "-admission-timeout", "200ms", "-request-timeout", "500ms")
		recoveryHTTPStatus(t, p.url+"/health", 200)
		client := &http.Client{Timeout: 2 * time.Second}
		status, err := admissionProfileStatusAt(client, p.url)
		if err != nil || status.Posted != 1 || status.RawBytes != int64(len(f.raw)) {
			t.Fatal("recovered posted work did not retain its byte/count reservation")
		}
		raw, _ := admissionProfileRaw(t, 1)
		code, reply, err := admissionProfileCall(client, p.url, raw)
		if err != nil || code != 200 || reply == nil || reply.Error == nil || reply.Error.Code != -32005 {
			t.Fatal("posted work no longer consumed capacity")
		}
		connection, err := net.DialTimeout("tcp", strings.TrimPrefix(p.url, "http://"), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		if err = connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(connection)
		if _, err = fmt.Fprint(connection, "POST / HTTP/1.1\r\nHost: fixture.invalid\r\nContent-Length: 100\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		continuing, err := http.ReadResponse(reader, nil)
		if err != nil || continuing.StatusCode != http.StatusContinue {
			t.Fatalf("body-reader admission was not observed: %v", err)
		}
		_ = continuing.Body.Close()
		started := time.Now()
		if _, err = fmt.Fprint(connection, "{"); err != nil {
			t.Fatal(err)
		}
		recoveryHTTPStatus(t, p.url+"/health", 200)
		query := `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`
		busy, err := client.Post(p.url, "application/json", strings.NewReader(query))
		if err != nil {
			t.Fatal(err)
		}
		_ = busy.Body.Close()
		if busy.StatusCode != http.StatusServiceUnavailable {
			t.Fatal("slow body did not occupy its one declared request slot")
		}
		timedOut, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = timedOut.Body.Close()
		elapsed := time.Since(started)
		if timedOut.StatusCode != http.StatusBadRequest || elapsed > 2*time.Second {
			t.Fatal("incomplete socket body did not release within the declared bound")
		}
		released := false
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			response, err := client.Post(p.url, "application/json", strings.NewReader(query))
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode == 200 {
				released = true
				break
			}
			if response.StatusCode != 503 {
				t.Fatal("unexpected post-deadline status")
			}
			time.Sleep(10 * time.Millisecond)
		}
		status, err = admissionProfileStatusAt(client, p.url)
		if !released || err != nil || status.Posted != 1 || status.Queued != 0 || status.RawBytes != int64(len(f.raw)) || f.writes.Load() != 0 {
			t.Fatal("request slot or posted reservation was lost")
		}
		_, peak, err := admissionProfileRSS(p.command.Process.Pid)
		if err != nil || peak > 384<<20 {
			t.Fatal("posted/slow-body process exceeded its RSS profile")
		}
		p.kill(t)
		record := map[string]any{"case": "actual_CLI_slow_body_and_posted_reservation", "posted_reservation_held": true, "retained_posted_bytes": status.RawBytes, "HTTP_backpressure_before_timeout": true,
			"health_reachable_while_busy": true, "incomplete_body_HTTP_status": 400, "socket_wait_milliseconds": elapsed.Milliseconds(), "post_deadline_slot_released": true,
			"kernel_peak_RSS_bytes": peak, "declared_RSS_ceiling_bytes": 384 << 20, "process_joined": true, "write_RPCs": 0}
		rawRecord, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(string(rawRecord))
	})
	admissionProfileCheckNoOOM(t, before)
	record := map[string]any{"case": "hard_container_resource_profile", "memory_max_bytes": 2 << 30, "CPUs": 2, "pids_max": 128, "OOM_events": 0, "child_GOMEMLIMIT": "128MiB", "declared_child_RSS_ceiling_bytes": 384 << 20, "production_binary_builds": 1, "real_chain_or_wallet_qualification": false}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
}
