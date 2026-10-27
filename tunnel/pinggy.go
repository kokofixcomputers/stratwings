package tunnel

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// Pinggy opens tunnels with an SSH connection to Pinggy (what `ssh -p 443 -R0:localhost:25565 tcp@a.pinggy.io` does), with the SSH client
// that is in Wings: nothing is installed on the node. Each tunnel is a connection of its own. The first host key that is seen is kept in
// a file and has to be the same from then on.
type Pinggy struct {
	dir  string
	addr string
}

func NewPinggy(dir string) *Pinggy {
	return &Pinggy{dir: dir, addr: "a.pinggy.io:443"}
}

var pinggyAddress = regexp.MustCompile(`tcp://([A-Za-z0-9.-]+:\d+)`)

type pinggyTunnel struct {
	client  *ssh.Client
	address string
	done    chan struct{}
	once    sync.Once
}

func (t *pinggyTunnel) Address() string       { return t.address }
func (t *pinggyTunnel) Done() <-chan struct{} { return t.done }
func (t *pinggyTunnel) Close() error {
	t.once.Do(func() { close(t.done) })
	return t.client.Close()
}

// hostKey remembers the key of the first connection and refuses another one after it.
func (p *Pinggy) hostKey(_ string, _ net.Addr, key ssh.PublicKey) error {
	path := filepath.Join(p.dir, "pinggy_host_key")
	marshaled := ssh.MarshalAuthorizedKey(key)

	known, err := os.ReadFile(path)
	if err == nil {
		if !bytes.Equal(bytes.TrimSpace(known), bytes.TrimSpace(marshaled)) {
			return errors.New("the host key of Pinggy is not the one it had before, the connection was not made")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(p.dir, 0o700); err != nil {
		return err
	}

	return os.WriteFile(path, marshaled, 0o600)
}

// Start forwards a TCP address that Pinggy gives out to the upstream.
func (p *Pinggy) Start(ctx context.Context, spec Spec) (Open, error) {
	user := "tcp"
	if spec.Token != "" {
		user = spec.Token + "+tcp"
	}

	client, err := ssh.Dial("tcp", p.addr, &ssh.ClientConfig{
		User: user,
		// Pinggy takes the token from the user name and does not look at the password.
		Auth:            []ssh.AuthMethod{ssh.Password("")},
		HostKeyCallback: p.hostKey,
		Timeout:         20 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to Pinggy: %w", err)
	}

	fail := func(err error) (Open, error) {
		_ = client.Close()
		return nil, err
	}

	listener, err := client.Listen("tcp", "localhost:0")
	if err != nil {
		return fail(fmt.Errorf("Pinggy did not open the tunnel: %w", err))
	}

	// The address is told on the session, the way the ssh command prints it.
	session, err := client.NewSession()
	if err != nil {
		return fail(err)
	}
	output, err := session.StdoutPipe()
	if err != nil {
		return fail(err)
	}
	if err := session.Shell(); err != nil {
		return fail(err)
	}

	found := make(chan string, 1)
	var seen bytes.Buffer
	var seenMu sync.Mutex
	go func() {
		reader := bufio.NewReader(output)
		for {
			line, err := reader.ReadString('\n')
			seenMu.Lock()
			seen.WriteString(line)
			seenMu.Unlock()
			if m := pinggyAddress.FindStringSubmatch(line); m != nil {
				found <- m[1]
				// Keep reading so the session does not block on its output.
				_, _ = io.Copy(io.Discard, reader)
				return
			}
			if err != nil {
				return
			}
		}
	}()

	tunnel := &pinggyTunnel{client: client, done: make(chan struct{})}
	select {
	case tunnel.address = <-found:
	case <-time.After(30 * time.Second):
		seenMu.Lock()
		said := strings.TrimSpace(seen.String())
		seenMu.Unlock()
		if said == "" {
			said = "it did not say anything"
		}
		return fail(fmt.Errorf("Pinggy did not give an address: %s", firstLine(said)))
	case <-ctx.Done():
		return fail(ctx.Err())
	}

	go func() {
		defer tunnel.once.Do(func() { close(tunnel.done) })
		go keepAlive(client, tunnel.done)
		go forward(listener, spec.Upstream)
		_ = client.Wait()
		_ = listener.Close()
	}()

	return tunnel, nil
}

func firstLine(text string) string {
	if i := strings.IndexAny(text, "\r\n"); i > 0 {
		text = text[:i]
	}
	return text
}

// keepAlive keeps the connection from being closed for being quiet.
func keepAlive(client *ssh.Client, done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if _, _, err := client.SendRequest("keepalive@openssh.com", true, nil); err != nil {
				_ = client.Close()
				return
			}
		}
	}
}

// forward passes every connection that comes in through the tunnel on to the upstream.
func forward(listener net.Listener, upstream string) {
	for {
		in, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer in.Close()
			out, err := net.DialTimeout("tcp", upstream, 10*time.Second)
			if err != nil {
				return
			}
			defer out.Close()

			done := make(chan struct{}, 2)
			go func() { _, _ = io.Copy(out, in); done <- struct{}{} }()
			go func() { _, _ = io.Copy(in, out); done <- struct{}{} }()
			<-done
		}()
	}
}
