package tunnel

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// fakePinggy is an SSH server that behaves like the part of Pinggy that matters: it takes the forward request, says an address on the
// session, and opens a channel back when somebody "connects" to that address.
type fakePinggy struct {
	listener net.Listener
	config   *ssh.ServerConfig
	user     chan string
	conns    chan ssh.Conn
}

func newFakePinggy(t *testing.T, say string) *fakePinggy {
	t.Helper()
	_, private, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := ssh.NewSignerFromKey(private)
	f := &fakePinggy{user: make(chan string, 4), conns: make(chan ssh.Conn, 4)}
	f.config = &ssh.ServerConfig{PasswordCallback: func(c ssh.ConnMetadata, _ []byte) (*ssh.Permissions, error) {
		f.user <- c.User()
		return nil, nil
	}}
	f.config.AddHostKey(signer)
	f.listener, _ = net.Listen("tcp", "127.0.0.1:0")
	go func() {
		for {
			raw, err := f.listener.Accept()
			if err != nil {
				return
			}
			go f.serve(raw, say)
		}
	}()
	t.Cleanup(func() { f.listener.Close() })

	return f
}

func (f *fakePinggy) serve(raw net.Conn, say string) {
	conn, channels, requests, err := ssh.NewServerConn(raw, f.config)
	if err != nil {
		return
	}
	f.conns <- conn
	go func() {
		for r := range requests {
			if r.Type == "tcpip-forward" {
				// The reply holds the port that was given out.
				reply := make([]byte, 4)
				binary.BigEndian.PutUint32(reply, 12345)
				_ = r.Reply(true, reply)
			} else if r.WantReply {
				_ = r.Reply(true, nil)
			}
		}
	}()
	for newChannel := range channels {
		channel, reqs, err := newChannel.Accept()
		if err != nil {
			return
		}
		go func() {
			for r := range reqs {
				if r.Type == "shell" {
					_ = r.Reply(true, nil)
					_, _ = io.WriteString(channel, say)
				} else if r.WantReply {
					_ = r.Reply(true, nil)
				}
			}
		}()
	}
}

func TestPinggyGivesTheAddressAndForwardsConnections(t *testing.T) {
	// The server on this machine that the tunnel leads to.
	upstream, _ := net.Listen("tcp", "127.0.0.1:0")
	defer upstream.Close()
	got := make(chan string, 1)
	go func() {
		c, err := upstream.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 4)
		_, _ = io.ReadFull(c, buf)
		got <- string(buf)
		_, _ = c.Write([]byte("pong"))
	}()

	fake := newFakePinggy(t, "Pinggy\r\nURLs:\r\n  tcp://abc-def.a.pinggy.link:34567\r\n")
	p := &Pinggy{dir: t.TempDir(), addr: fake.listener.Addr().String()}

	open, err := p.Start(context.Background(), Spec{Token: "mytoken", Upstream: upstream.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()

	if open.Address() != "abc-def.a.pinggy.link:34567" {
		t.Fatalf("address was %q", open.Address())
	}
	select {
	case user := <-fake.user:
		if user != "mytoken+tcp" {
			t.Fatalf("the token should be in the user name, got %q", user)
		}
	case <-time.After(time.Second):
		t.Fatal("no login")
	}

	// A player connecting to the address: the server opens a channel to the client.
	conn := <-fake.conns
	payload := ssh.Marshal(struct {
		Addr       string
		Port       uint32
		OriginAddr string
		OriginPort uint32
	}{"localhost", 12345, "203.0.113.5", 50000})
	channel, reqs, err := conn.OpenChannel("forwarded-tcpip", payload)
	if err != nil {
		t.Fatalf("the tunnel did not take the connection: %v", err)
	}
	go ssh.DiscardRequests(reqs)
	_, _ = channel.Write([]byte("ping"))
	if received := <-got; received != "ping" {
		t.Fatalf("upstream got %q", received)
	}
	answer := make([]byte, 4)
	_, _ = io.ReadFull(channel, answer)
	if string(answer) != "pong" {
		t.Fatalf("the answer was %q", answer)
	}

	_ = open.Close()
	select {
	case <-open.Done():
	case <-time.After(time.Second):
		t.Fatal("Done was not closed")
	}
}

func TestPinggyWithoutATokenIsTheFreePlan(t *testing.T) {
	fake := newFakePinggy(t, "tcp://x.a.pinggy.link:1\r\n")
	p := &Pinggy{dir: t.TempDir(), addr: fake.listener.Addr().String()}

	open, err := p.Start(context.Background(), Spec{Token: "", Upstream: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	if user := <-fake.user; user != "tcp" {
		t.Fatalf("got user %q", user)
	}
}

func TestPinggyRefusesAnotherHostKey(t *testing.T) {
	dir := t.TempDir()
	first := newFakePinggy(t, "tcp://x.a.pinggy.link:1\r\n")
	p := &Pinggy{dir: dir, addr: first.listener.Addr().String()}
	open, err := p.Start(context.Background(), Spec{Token: "", Upstream: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	_ = open.Close()
	if _, err := os.Stat(filepath.Join(dir, "pinggy_host_key")); err != nil {
		t.Fatalf("the host key was not kept: %v", err)
	}

	// Another server answers on the address now.
	second := newFakePinggy(t, "tcp://y.a.pinggy.link:2\r\n")
	p.addr = second.listener.Addr().String()
	_, err = p.Start(context.Background(), Spec{Token: "", Upstream: "127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "host key") {
		t.Fatalf("a different host key should be refused, got %v", err)
	}
}
