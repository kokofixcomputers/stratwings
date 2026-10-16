// Package selfupdate replaces the running Wings binary with the one of a release and keeps the old one to go back to.
package selfupdate

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// DefaultBaseURL is where the nightly build of the fork is published. WINGS_UPDATE_URL overrides it.
const DefaultBaseURL = "https://github.com/kokofixcomputers/stratwings/releases/download/nightly"

const maxBinarySize = 200 << 20

// Result tells what an update did.
type Result struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Updated bool   `json:"updated"`
}

// Options are the inputs of an update. Empty values take the defaults.
type Options struct {
	BaseURL string
	Current string
	Arch    string
	Exe     string
	Client  *http.Client
}

func (o *Options) defaults() error {
	if o.BaseURL == "" {
		o.BaseURL = os.Getenv("WINGS_UPDATE_URL")
	}
	if o.BaseURL == "" {
		o.BaseURL = DefaultBaseURL
	}
	o.BaseURL = strings.TrimRight(o.BaseURL, "/")
	if o.Client == nil {
		o.Client = &http.Client{Timeout: 5 * time.Minute}
	}
	if o.Exe == "" {
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		o.Exe = exe
	}
	return nil
}

func get(ctx context.Context, c *http.Client, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		return nil, fmt.Errorf("%s answered %d", url, res.StatusCode)
	}
	return res, nil
}

func text(ctx context.Context, c *http.Client, url string) (string, error) {
	res, err := get(ctx, c, url)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return strings.TrimSpace(string(b)), err
}

// Latest is the version the release holds.
func Latest(ctx context.Context, o Options) (string, error) {
	if err := o.defaults(); err != nil {
		return "", err
	}
	return text(ctx, o.Client, o.BaseURL+"/version.txt")
}

// Checksum finds the hash of a file in the text of a checksums.txt (sha256sum format).
func Checksum(sums, name string) (string, bool) {
	sc := bufio.NewScanner(strings.NewReader(sums))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 2 && strings.TrimPrefix(f[1], "*") == name {
			return strings.ToLower(f[0]), true
		}
	}
	return "", false
}

// Run downloads the release, checks it and puts it in place of the executable. The process does not restart here: see Restart.
func Run(ctx context.Context, o Options) (Result, error) {
	if err := o.defaults(); err != nil {
		return Result{}, err
	}
	res := Result{From: o.Current, To: o.Current}
	if o.Arch == "" {
		return res, errors.New("no architecture given")
	}

	latest, err := text(ctx, o.Client, o.BaseURL+"/version.txt")
	if err != nil {
		return res, fmt.Errorf("could not find out the latest version: %w", err)
	}
	if latest == "" {
		return res, errors.New("the release has an empty version")
	}
	res.To = latest
	if latest == o.Current {
		return res, nil
	}

	name := "wings_linux_" + o.Arch
	sums, err := text(ctx, o.Client, o.BaseURL+"/checksums.txt")
	if err != nil {
		return res, fmt.Errorf("could not read the checksums: %w", err)
	}
	want, ok := Checksum(sums, name)
	if !ok {
		return res, fmt.Errorf("the release has no build for %s", o.Arch)
	}

	staged := o.Exe + ".new"
	if err := download(ctx, o, o.BaseURL+"/"+name, staged, want); err != nil {
		os.Remove(staged)
		return res, err
	}

	// Run it once: a build that does not start must never replace the one that does.
	out, err := exec.CommandContext(ctx, staged, "version").CombinedOutput()
	if err != nil || !strings.Contains(string(out), latest) {
		os.Remove(staged)
		return res, fmt.Errorf("the new binary does not run (%v): %s", err, strings.TrimSpace(string(out)))
	}

	old := o.Exe + ".old"
	os.Remove(old)
	if err := os.Link(o.Exe, old); err != nil {
		if err := copyFile(o.Exe, old); err != nil {
			os.Remove(staged)
			return res, fmt.Errorf("could not keep the old binary: %w", err)
		}
	}
	if err := os.Rename(staged, o.Exe); err != nil {
		os.Remove(staged)
		return res, err
	}
	// Marks the new binary as not yet proven, see Boot.
	_ = os.WriteFile(o.Exe+".updating", []byte("0"), 0o600)

	res.Updated = true
	return res, nil
}

func download(ctx context.Context, o Options, url, to, want string) error {
	r, err := get(ctx, o.Client, url)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	f, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(r.Body, maxBinarySize+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if n > maxBinarySize {
		return errors.New("the download is too large")
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		return fmt.Errorf("the download does not match its checksum (%s, expected %s)", got, want)
	}
	return nil
}

func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// Restart starts the executable in place of this process.
func Restart(exe string) error {
	return syscall.Exec(exe, os.Args, os.Environ())
}

// Boot runs when Wings starts. A freshly updated binary is on trial: if it comes up three times without staying up for a minute, the
// old one is put back. It returns true when it did so, after which the process has to restart.
func Boot(exe string) (rolledBack bool) {
	marker := exe + ".updating"
	b, err := os.ReadFile(marker)
	if err != nil {
		return false
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	n++
	if n > 3 {
		if _, err := os.Stat(exe + ".old"); err == nil && os.Rename(exe+".old", exe) == nil {
			os.Remove(marker)
			return true
		}
		os.Remove(marker)
		return false
	}
	_ = os.WriteFile(marker, []byte(strconv.Itoa(n)), 0o600)
	time.AfterFunc(time.Minute, func() { os.Remove(marker) })
	return false
}
