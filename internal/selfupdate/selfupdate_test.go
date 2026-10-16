package selfupdate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func release(t *testing.T, version string, binary string) *httptest.Server {
	sum := sha256.Sum256([]byte(binary))
	mux := http.NewServeMux()
	mux.HandleFunc("/version.txt", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(version + "\n")) })
	mux.HandleFunc("/checksums.txt", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(hex.EncodeToString(sum[:]) + "  wings_linux_amd64\n"))
	})
	mux.HandleFunc("/wings_linux_amd64", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(binary)) })
	return httptest.NewServer(mux)
}

func TestChecksum(t *testing.T) {
	sums := "AA11  wings_linux_amd64\nbb22 *wings_linux_arm64\n"
	if h, ok := Checksum(sums, "wings_linux_amd64"); !ok || h != "aa11" {
		t.Fatalf("got %q %v", h, ok)
	}
	if h, ok := Checksum(sums, "wings_linux_arm64"); !ok || h != "bb22" {
		t.Fatalf("got %q %v", h, ok)
	}
	if _, ok := Checksum(sums, "other"); ok {
		t.Fatal("found a file that is not there")
	}
}

func TestSameVersionChangesNothing(t *testing.T) {
	srv := release(t, "nightly-1", "x")
	defer srv.Close()
	exe := filepath.Join(t.TempDir(), "wings")
	os.WriteFile(exe, []byte("old"), 0o755)
	r, err := Run(context.Background(), Options{BaseURL: srv.URL, Current: "nightly-1", Arch: "amd64", Exe: exe})
	if err != nil || r.Updated {
		t.Fatalf("%+v %v", r, err)
	}
	if b, _ := os.ReadFile(exe); string(b) != "old" {
		t.Fatal("the binary changed")
	}
}

func TestBadChecksumKeepsTheOldBinary(t *testing.T) {
	srv := release(t, "nightly-2", "x")
	defer srv.Close()
	// Serve something else than what the checksum is of.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/wings_linux_amd64" {
			w.Write([]byte("tampered"))
			return
		}
		http.Redirect(w, r, srv.URL+r.URL.Path, http.StatusFound)
	}))
	defer bad.Close()
	exe := filepath.Join(t.TempDir(), "wings")
	os.WriteFile(exe, []byte("old"), 0o755)
	if _, err := Run(context.Background(), Options{BaseURL: bad.URL, Current: "nightly-1", Arch: "amd64", Exe: exe}); err == nil {
		t.Fatal("accepted a download that does not match")
	}
	if b, _ := os.ReadFile(exe); string(b) != "old" {
		t.Fatal("the binary changed")
	}
	if _, err := os.Stat(exe + ".new"); err == nil {
		t.Fatal("left the download behind")
	}
}

func TestUpdateReplacesAndKeepsOld(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	srv := release(t, "nightly-3", "#!/bin/sh\necho wings vnightly-3\n")
	defer srv.Close()
	exe := filepath.Join(t.TempDir(), "wings")
	os.WriteFile(exe, []byte("old"), 0o755)
	r, err := Run(context.Background(), Options{BaseURL: srv.URL, Current: "nightly-1", Arch: "amd64", Exe: exe})
	if err != nil || !r.Updated || r.To != "nightly-3" {
		t.Fatalf("%+v %v", r, err)
	}
	if b, _ := os.ReadFile(exe + ".old"); string(b) != "old" {
		t.Fatal("the old binary was not kept")
	}
	if _, err := os.Stat(exe + ".updating"); err != nil {
		t.Fatal("no trial marker")
	}
}

func TestBinaryThatDoesNotRunIsRefused(t *testing.T) {
	srv := release(t, "nightly-4", "#!/bin/sh\nexit 3\n")
	defer srv.Close()
	exe := filepath.Join(t.TempDir(), "wings")
	os.WriteFile(exe, []byte("old"), 0o755)
	if _, err := Run(context.Background(), Options{BaseURL: srv.URL, Current: "nightly-1", Arch: "amd64", Exe: exe}); err == nil {
		t.Fatal("accepted a binary that fails")
	}
	if b, _ := os.ReadFile(exe); string(b) != "old" {
		t.Fatal("the binary changed")
	}
}

func TestBootRollsBackAfterThreeFailedStarts(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "wings")
	os.WriteFile(exe, []byte("new"), 0o755)
	os.WriteFile(exe+".old", []byte("old"), 0o755)
	os.WriteFile(exe+".updating", []byte("0"), 0o600)
	for i := 0; i < 3; i++ {
		if Boot(exe) {
			t.Fatalf("rolled back too early at start %d", i+1)
		}
	}
	if !Boot(exe) {
		t.Fatal("did not roll back")
	}
	if b, _ := os.ReadFile(exe); string(b) != "old" {
		t.Fatal("the old binary is not back")
	}
	if Boot(exe) {
		t.Fatal("rolled back again")
	}
}
