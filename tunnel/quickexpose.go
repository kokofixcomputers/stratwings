package tunnel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// QuickExpose opens tunnels through a QuickExpose relay that the admin runs (the relay of the QuickExpose mod). There is no account and no
// default relay: the backend is its address, `host`, or `host:control:tunnel:play` for ports other than 7000, 7001 and 25565.
//
// The protocol is the one of the mod: a control connection says `REGISTER <slug> <port>`, the relay answers `OK <slug>.<domain>` and sends
// `TUNNEL <session>` when a player connects, for which a new connection to the tunnel port of the relay says `TUNNEL <session>` and is
// spliced to the port of the server.
type QuickExpose struct{}

func NewQuickExpose() *QuickExpose { return &QuickExpose{} }

// Relay is where a QuickExpose relay listens.
type Relay struct {
	Host                  string
	Control, Tunnel, Play int
}

// ParseRelay reads a backend: `host`, `host:control`, `host:control:tunnel` or `host:control:tunnel:play`, with a scheme in front that is
// not needed (qe://, tcp://, https://).
func ParseRelay(backend string) (Relay, error) {
	text := strings.TrimSpace(backend)
	if i := strings.Index(text, "://"); i >= 0 {
		text = text[i+3:]
	}
	text = strings.TrimRight(text, "/")
	if text == "" {
		return Relay{}, errors.New("No backend URL is set in the Panel.")
	}

	parts := strings.Split(text, ":")
	if len(parts) > 4 {
		return Relay{}, fmt.Errorf("the backend %q is not host or host:control:tunnel:play", backend)
	}
	relay := Relay{Host: parts[0], Control: 7000, Tunnel: 7001, Play: 25565}
	if relay.Host == "" || strings.ContainsAny(relay.Host, " /?#@") {
		return Relay{}, fmt.Errorf("the backend %q has no host", backend)
	}
	for i, target := range []*int{&relay.Control, &relay.Tunnel, &relay.Play} {
		if len(parts) > i+1 && parts[i+1] != "" {
			port, err := strconv.Atoi(parts[i+1])
			if err != nil || port < 1 || port > 65535 {
				return Relay{}, fmt.Errorf("the backend %q has a port that is not a port: %q", backend, parts[i+1])
			}
			*target = port
		}
	}

	return relay, nil
}

var qeAdjectives = []string{"amber", "azure", "brave", "bright", "calm", "clear", "cool", "dark", "deep", "fast", "fierce", "fresh", "golden",
	"green", "happy", "hidden", "iron", "jade", "kind", "large", "light", "loud", "lucky", "mighty", "misty", "mystic", "noble", "north", "proud",
	"quick", "quiet", "rapid", "red", "rough", "royal", "silent", "silver", "sleek", "smooth", "solar", "still", "stone", "strong", "swift", "tall",
	"teal", "tiny", "tough", "vivid", "warm", "wild", "wise"}

var qeNouns = []string{"arrow", "badger", "beacon", "bear", "blade", "bolt", "brook", "cave", "cliff", "cloud", "comet", "creek", "dawn", "deer",
	"dune", "eagle", "ember", "falcon", "fern", "flame", "flash", "fox", "frost", "gate", "glacier", "grove", "hawk", "hill", "hollow", "isle",
	"lake", "leaf", "lion", "marsh", "mist", "moon", "moss", "oak", "peak", "pine", "pond", "raven", "reef", "ridge", "river", "rock", "rose",
	"sage", "shadow", "shore", "sky", "snow", "spark", "star", "storm", "stream", "tiger", "vale", "vine", "wave", "wind", "wolf", "wood"}

// Slug is the name a tunnel asks the relay for: two words with a hyphen (the relay keeps names without one for players), the same for the
// same tunnel, so a server has the same address every time it starts when nobody has taken it. The relay gives another when it is taken.
func Slug(id int) string {
	h := fnv.New32a()
	_, _ = fmt.Fprintf(h, "stratosphere-tunnel-%d", id)
	n := h.Sum32()

	return qeAdjectives[n%uint32(len(qeAdjectives))] + "-" + qeNouns[(n/uint32(len(qeAdjectives)))%uint32(len(qeNouns))]
}

type qeTunnel struct {
	conn    net.Conn
	address string
	done    chan struct{}
	once    sync.Once
}

func (t *qeTunnel) Address() string       { return t.address }
func (t *qeTunnel) Done() <-chan struct{} { return t.done }
func (t *qeTunnel) Close() error {
	t.once.Do(func() { close(t.done) })
	return t.conn.Close()
}

// Start registers with the relay and keeps the control connection, which is what the tunnel is.
func (q *QuickExpose) Start(ctx context.Context, spec Spec) (Open, error) {
	relay, err := ParseRelay(spec.Backend)
	if err != nil {
		return nil, err
	}
	_, portText, err := net.SplitHostPort(spec.Upstream)
	if err != nil {
		return nil, err
	}

	dialer := net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(relay.Host, strconv.Itoa(relay.Control)))
	if err != nil {
		return nil, fmt.Errorf("could not reach the QuickExpose relay %s:%d: %w", relay.Host, relay.Control, err)
	}
	if _, err := fmt.Fprintf(conn, "REGISTER %s %s\n", Slug(spec.ID), portText); err != nil {
		_ = conn.Close()
		return nil, err
	}

	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	line, err := reader.ReadString('\n')
	_ = conn.SetReadDeadline(time.Time{})
	fields := strings.Fields(line)
	if err != nil || len(fields) < 2 || fields[0] != "OK" {
		_ = conn.Close()
		if err == nil {
			err = fmt.Errorf("the relay answered %q", strings.TrimSpace(line))
		}
		return nil, fmt.Errorf("the QuickExpose relay did not accept the tunnel: %w", err)
	}

	tunnel := &qeTunnel{conn: conn, done: make(chan struct{}), address: net.JoinHostPort(fields[1], strconv.Itoa(relay.Play))}

	go func() {
		defer tunnel.once.Do(func() { close(tunnel.done) })
		defer conn.Close()
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			parts := strings.Fields(line)
			if len(parts) == 0 {
				continue
			}
			switch parts[0] {
			case "PING":
				if _, err := io.WriteString(conn, "PONG\n"); err != nil {
					return
				}
			case "TUNNEL":
				if len(parts) >= 2 {
					go carry(relay, parts[1], spec.Upstream)
				}
			}
		}
	}()

	return tunnel, nil
}

// carry is one player: a new connection to the relay that says which session it is, spliced to the server.
func carry(relay Relay, session, upstream string) {
	out, err := net.DialTimeout("tcp", net.JoinHostPort(relay.Host, strconv.Itoa(relay.Tunnel)), 10*time.Second)
	if err != nil {
		return
	}
	defer out.Close()
	local, err := net.DialTimeout("tcp", upstream, 10*time.Second)
	if err != nil {
		return
	}
	defer local.Close()

	if _, err := fmt.Fprintf(out, "TUNNEL %s\n", session); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(local, out); done <- struct{}{} }()
	go func() { _, _ = io.Copy(out, local); done <- struct{}{} }()
	<-done
}
