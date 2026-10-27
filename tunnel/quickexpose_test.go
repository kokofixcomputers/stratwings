package tunnel

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestParseRelay(t *testing.T) {
	cases := map[string]Relay{
		"qe.example.com":                 {"qe.example.com", 7000, 7001, 25565},
		"qe://qe.example.com":            {"qe.example.com", 7000, 7001, 25565},
		"https://qe.example.com/":        {"qe.example.com", 7000, 7001, 25565},
		"qe.example.com:7100":            {"qe.example.com", 7100, 7001, 25565},
		"qe.example.com:7100:7101":       {"qe.example.com", 7100, 7101, 25565},
		"qe.example.com:7100:7101:25570": {"qe.example.com", 7100, 7101, 25570},
		"qe.example.com::7101":           {"qe.example.com", 7000, 7101, 25565},
		" 10.0.0.9 ":                     {"10.0.0.9", 7000, 7001, 25565},
	}
	for in, want := range cases {
		got, err := ParseRelay(in)
		if err != nil || got != want {
			t.Errorf("ParseRelay(%q) = %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "   ", "a:b", "a:70000", "a:1:2:3:4", "bad host", "a:0"} {
		if _, err := ParseRelay(bad); err == nil {
			t.Errorf("ParseRelay(%q) should fail", bad)
		}
	}
}

func TestSlugIsTheSameForATunnelAndHasAHyphen(t *testing.T) {
	if Slug(7) != Slug(7) {
		t.Fatalf("a tunnel should ask for the same name every time")
	}
	seen := map[string]bool{}
	for id := 1; id <= 40; id++ {
		slug := Slug(id)
		if !strings.Contains(slug, "-") || strings.ToLower(slug) != slug {
			t.Fatalf("%q is not a two word name", slug)
		}
		seen[slug] = true
	}
	if len(seen) < 30 {
		t.Fatalf("the names should be spread out, only %d of 40 differ", len(seen))
	}
}

// fakeRelay speaks the protocol of the relay of QuickExpose: registers a name, tells a session when somebody connects, splices a tunnel.
type fakeRelay struct {
	control, tunnel net.Listener
	registered      chan string
	domain          string
	pong            chan struct{}
}

func newFakeRelay(t *testing.T, domain string) *fakeRelay {
	t.Helper()
	f := &fakeRelay{registered: make(chan string, 4), domain: domain, pong: make(chan struct{}, 4)}
	f.control, _ = net.Listen("tcp", "127.0.0.1:0")
	f.tunnel, _ = net.Listen("tcp", "127.0.0.1:0")
	t.Cleanup(func() { f.control.Close(); f.tunnel.Close() })

	return f
}

func (f *fakeRelay) backend(play int) string {
	_, c, _ := net.SplitHostPort(f.control.Addr().String())
	_, tn, _ := net.SplitHostPort(f.tunnel.Addr().String())
	return fmt.Sprintf("127.0.0.1:%s:%s:%d", c, tn, play)
}

func TestQuickExposeRegistersAndCarriesAPlayer(t *testing.T) {
	// The server on this machine.
	upstream, _ := net.Listen("tcp", "127.0.0.1:0")
	defer upstream.Close()
	got := make(chan string, 1)
	go func() {
		c, err := upstream.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 8)
		_, _ = io.ReadFull(c, buf)
		got <- string(buf)
		_, _ = c.Write([]byte("welcome!"))
	}()

	relay := newFakeRelay(t, "qe.example.com")
	controlConn := make(chan net.Conn, 1)
	go func() {
		c, err := relay.control.Accept()
		if err != nil {
			return
		}
		sc := bufio.NewScanner(c)
		sc.Scan()
		relay.registered <- sc.Text()
		_, _ = fmt.Fprintf(c, "OK %s.%s\n", strings.Fields(sc.Text())[1], relay.domain)
		controlConn <- c
		for sc.Scan() {
			if sc.Text() == "PONG" {
				relay.pong <- struct{}{}
			}
		}
	}()
	// The tunnel port: what the player brings, after the handshake that the relay replays.
	player := make(chan net.Conn, 1)
	go func() {
		c, err := relay.tunnel.Accept()
		if err != nil {
			return
		}
		sc := bufio.NewReader(c)
		line, _ := sc.ReadString('\n')
		if strings.TrimSpace(line) != "TUNNEL session42" {
			c.Close()
			return
		}
		player <- c
	}()

	q := NewQuickExpose()
	open, err := q.Start(context.Background(), Spec{ID: 5, Upstream: upstream.Addr().String(), Backend: relay.backend(25570)})
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()

	registered := <-relay.registered
	_, port, _ := net.SplitHostPort(upstream.Addr().String())
	if registered != "REGISTER "+Slug(5)+" "+port {
		t.Fatalf("registered %q", registered)
	}
	if open.Address() != Slug(5)+".qe.example.com:25570" {
		t.Fatalf("address %q", open.Address())
	}

	// A keepalive is answered, and a player connecting is carried to the server.
	control := <-controlConn
	_, _ = io.WriteString(control, "PING\n")
	select {
	case <-relay.pong:
	case <-time.After(time.Second):
		t.Fatal("PING was not answered")
	}
	_, _ = io.WriteString(control, "TUNNEL session42\n")
	var c net.Conn
	select {
	case c = <-player:
	case <-time.After(2 * time.Second):
		t.Fatal("no connection to the tunnel port")
	}
	_, _ = c.Write([]byte("hello mc"))
	if received := <-got; received != "hello mc" {
		t.Fatalf("the server got %q", received)
	}
	answer := make([]byte, 8)
	_, _ = io.ReadFull(c, answer)
	if string(answer) != "welcome!" {
		t.Fatalf("the player got %q", answer)
	}

	// The tunnel is gone when the relay drops the control connection.
	_ = control.Close()
	select {
	case <-open.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done was not closed")
	}
}

func TestQuickExposeSaysWhenTheRelayCanNotBeReachedOrRefuses(t *testing.T) {
	q := NewQuickExpose()
	if _, err := q.Start(context.Background(), Spec{ID: 1, Upstream: "127.0.0.1:25565", Backend: ""}); err == nil || err.Error() != "No backend URL is set in the Panel." {
		t.Fatalf("got %v", err)
	}

	free, _ := net.Listen("tcp", "127.0.0.1:0")
	_, p, _ := net.SplitHostPort(free.Addr().String())
	free.Close()
	if _, err := q.Start(context.Background(), Spec{ID: 1, Upstream: "127.0.0.1:25565", Backend: "127.0.0.1:" + p}); err == nil || !strings.Contains(err.Error(), "could not reach the QuickExpose relay") {
		t.Fatalf("got %v", err)
	}

	// A relay that closes the connection without an OK.
	closer, _ := net.Listen("tcp", "127.0.0.1:0")
	defer closer.Close()
	go func() {
		c, err := closer.Accept()
		if err == nil {
			c.Close()
		}
	}()
	_, cp, _ := net.SplitHostPort(closer.Addr().String())
	if _, err := q.Start(context.Background(), Spec{ID: 1, Upstream: "127.0.0.1:25565", Backend: "127.0.0.1:" + cp}); err == nil || !strings.Contains(err.Error(), "did not accept") {
		t.Fatalf("got %v", err)
	}
}

func TestManagerNeedsABackendForQuickExpose(t *testing.T) {
	source := &fakeSource{config: Config{Provider: QuickExposeProvider, Tunnels: []Tunnel{{ID: 1, Port: 25565, Server: "a"}}}}
	starter := &fakeStarter{}
	m := NewManager(source, map[string]Starter{QuickExposeProvider: starter})
	_ = m.Sync(context.Background())
	if m.Count() != 0 || len(source.reports) != 1 || source.reports[0] != "failed  No backend URL is set in the Panel." {
		t.Fatalf("no backend, no tunnel: %v", source.reports)
	}

	source.config.Backend = "qe.example.com"
	_ = m.Sync(context.Background())
	if m.Count() != 1 {
		t.Fatalf("with a backend and no token it should open")
	}
	// Another backend is another relay: the tunnel is opened again.
	source.config.Backend = "other.example.com"
	_ = m.Sync(context.Background())
	if len(starter.started) != 2 || !starter.opened[0].closed {
		t.Fatalf("a new backend should open the tunnels again")
	}
}
