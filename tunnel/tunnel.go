// Package tunnel opens the ports of servers on this node to the internet through a tunnel service (ngrok), so a server can be played
// without forwarding a port on the router. The Panel decides which ports (Network tab of a server); this node opens them and says
// how it went.
package tunnel

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/apex/log"
)

// Tunnel is a port of this node that the Panel wants reachable from the internet.
type Tunnel struct {
	ID   int
	IP   string
	Port int
}

// Config is what the Panel wants this node to have open: the token of the tunnel service, and the tunnels.
type Config struct {
	Token   string
	Tunnels []Tunnel
}

// Source is the Panel, as far as tunnels go.
type Source interface {
	Tunnels(ctx context.Context) (Config, error)
	ReportTunnel(ctx context.Context, id int, state, address, message string) error
}

// Open is a tunnel that is up.
type Open interface {
	// Address is where players connect, host:port.
	Address() string
	// Done is closed when the tunnel is gone.
	Done() <-chan struct{}
	Close() error
}

// Starter opens a tunnel from the internet to an address on this machine.
type Starter interface {
	Start(ctx context.Context, token, upstream string) (Open, error)
}

type running struct {
	open     Open
	upstream string
}

// Default is the manager of this node, set when it starts. The Panel asks it to look again when somebody opens or closes a tunnel.
var Default *Manager

// Manager keeps the tunnels the Panel asks for open, and closes the ones it does not ask for any more.
type Manager struct {
	source  Source
	starter Starter

	mu      sync.Mutex
	token   string
	tunnels map[int]*running
	synced  chan struct{}
}

func NewManager(source Source, starter Starter) *Manager {
	return &Manager{source: source, starter: starter, tunnels: map[int]*running{}, synced: make(chan struct{}, 1)}
}

// Upstream is the address of a port of this machine: a server that listens on every address is reached on the loopback one.
func Upstream(ip string, port int) string {
	if ip == "" || ip == "0.0.0.0" || ip == "::" {
		ip = "127.0.0.1"
	}
	return net.JoinHostPort(ip, fmt.Sprint(port))
}

// Sync makes the open tunnels the ones the Panel asks for.
func (m *Manager) Sync(ctx context.Context) error {
	config, err := m.source.Tunnels(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// A new token is a new account: everything is opened again with it.
	if config.Token != m.token {
		m.closeAll()
		m.token = config.Token
	}

	wanted := map[int]Tunnel{}
	for _, t := range config.Tunnels {
		wanted[t.ID] = t
	}

	for id, r := range m.tunnels {
		t, still := wanted[id]
		if still && Upstream(t.IP, t.Port) == r.upstream {
			continue
		}
		_ = r.open.Close()
		delete(m.tunnels, id)
	}

	for id, t := range wanted {
		if _, ok := m.tunnels[id]; ok {
			continue
		}
		if config.Token == "" {
			m.report(ctx, id, "failed", "", "No tunnel token is set in the Panel.")
			continue
		}

		upstream := Upstream(t.IP, t.Port)
		open, err := m.starter.Start(ctx, config.Token, upstream)
		if err != nil {
			log.WithField("tunnel", id).WithField("error", err).Warn("could not open a tunnel")
			m.report(ctx, id, "failed", "", cleanError(err))
			continue
		}
		m.tunnels[id] = &running{open: open, upstream: upstream}
		m.report(ctx, id, "active", open.Address(), "")
		go m.watch(id, open)
	}

	return nil
}

// watch tells the Panel when a tunnel goes away by itself, and lets the next sync open it again.
func (m *Manager) watch(id int, open Open) {
	<-open.Done()

	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.tunnels[id]; ok && r.open == open {
		delete(m.tunnels, id)
		m.report(context.Background(), id, "failed", "", "The tunnel was closed, it is opened again soon.")
	}
}

func (m *Manager) closeAll() {
	for id, r := range m.tunnels {
		_ = r.open.Close()
		delete(m.tunnels, id)
	}
}

func (m *Manager) report(ctx context.Context, id int, state, address, message string) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m.source.ReportTunnel(ctx, id, state, address, message); err != nil {
		log.WithField("tunnel", id).WithField("error", err).Warn("could not tell the Panel about a tunnel")
	}
}

// Run syncs now, and again every few minutes: that opens the tunnels that failed or went away, and finds the ones the Panel changed while this
// node could not be reached.
func (m *Manager) Run(ctx context.Context) {
	for {
		if err := m.Sync(ctx); err != nil {
			log.WithField("error", err).Debug("could not ask the Panel for tunnels")
		}

		select {
		case <-ctx.Done():
			m.mu.Lock()
			m.closeAll()
			m.mu.Unlock()
			return
		case <-time.After(3 * time.Minute):
		}
	}
}

// Count is how many tunnels are open.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.tunnels)
}

// cleanError is a message for the person looking at the Panel: the first line of what the service said, without its error codes' noise.
func cleanError(err error) string {
	text := strings.TrimSpace(err.Error())
	if i := strings.Index(text, "\n"); i > 0 {
		text = text[:i]
	}
	if len(text) > 300 {
		text = text[:300]
	}
	return text
}
