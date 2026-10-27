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
	// The server the port belongs to: its tunnel is open while it runs.
	Server string
}

// Config is what the Panel wants this node to have open: the token of the tunnel service, and the tunnels.
type Config struct {
	// The service the tunnels go through: ngrok, pinggy, playit or quickexpose. Empty is ngrok.
	Provider string
	Token    string
	// The address of the relay, for QuickExpose.
	Backend string
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

// Spec is what a tunnel is opened with.
type Spec struct {
	// The id the Panel gave the tunnel.
	ID int
	// The token of the service (not every service has one).
	Token string
	// The address on this machine the tunnel leads to.
	Upstream string
	// Where the service is, for the ones that are run by the admin (QuickExpose).
	Backend string
}

// Starter opens a tunnel from the internet to an address on this machine.
type Starter interface {
	Start(ctx context.Context, spec Spec) (Open, error)
}

// Providers are the tunnel services this node can use, by name.
const (
	NgrokProvider  = "ngrok"
	PinggyProvider = "pinggy"
	PlayitProvider = "playit"
	// QuickExpose is a relay the admin runs, its address is the backend.
	QuickExposeProvider = "quickexpose"
)

type running struct {
	open     Open
	upstream string
}

// Default is the manager of this node, set when it starts. The Panel asks it to look again when somebody opens or closes a tunnel.
var Default *Manager

// Manager keeps the tunnels the Panel asks for open, and closes the ones it does not ask for any more.
type Manager struct {
	source   Source
	starters map[string]Starter

	// Active says whether a server is running (or starting), which is when its tunnels are open. Nil means always.
	Active func(server string) bool

	mu       sync.Mutex
	config   Config
	provider string
	token    string
	backend  string
	tunnels  map[int]*running
	// The tunnels that failed to open, which are tried again at the next sync, and the ones the Panel was told are waiting.
	failed map[int]bool
	told   map[int]bool
}

func NewManager(source Source, starters map[string]Starter) *Manager {
	return &Manager{source: source, starters: starters, tunnels: map[int]*running{}, failed: map[int]bool{}, told: map[int]bool{}}
}

// Upstream is the address of a port of this machine: a server that listens on every address is reached on the loopback one.
func Upstream(ip string, port int) string {
	if ip == "" || ip == "0.0.0.0" || ip == "::" {
		ip = "127.0.0.1"
	}
	return net.JoinHostPort(ip, fmt.Sprint(port))
}

// Sync asks the Panel what it wants and makes the open tunnels those, for the servers that are running. It also gives the tunnels that failed
// another try.
func (m *Manager) Sync(ctx context.Context) error {
	config, err := m.source.Tunnels(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = config
	m.failed = map[int]bool{}
	m.apply(ctx)

	return nil
}

// Reconcile does the same with what the Panel said last, without asking: a tunnel is open while its server is running, so it follows the
// server when it starts and stops.
func (m *Manager) Reconcile(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.apply(ctx)
}

func (m *Manager) active(server string) bool {
	return m.Active == nil || m.Active(server)
}

// apply must be called with the lock held.
func (m *Manager) apply(ctx context.Context) {
	config := m.config
	provider := config.Provider
	if provider == "" {
		provider = NgrokProvider
	}

	// A new token is a new account, and another service is another way out: everything is opened again.
	if config.Token != m.token || provider != m.provider || config.Backend != m.backend {
		m.closeAll()
		m.token = config.Token
		m.provider = provider
		m.backend = config.Backend
	}

	wanted := map[int]Tunnel{}
	waiting := map[int]Tunnel{}
	for _, t := range config.Tunnels {
		if m.active(t.Server) {
			wanted[t.ID] = t
		} else {
			waiting[t.ID] = t
		}
	}

	// What is not wanted any more, or is for a server that stopped, is closed.
	for id, r := range m.tunnels {
		t, still := wanted[id]
		if still && Upstream(t.IP, t.Port) == r.upstream {
			continue
		}
		_ = r.open.Close()
		delete(m.tunnels, id)
	}

	// The tunnels of servers that are not running wait for them, and the Panel is told once.
	for id := range m.told {
		if _, ok := waiting[id]; !ok {
			delete(m.told, id)
		}
	}
	for id := range waiting {
		if !m.told[id] {
			m.told[id] = true
			m.report(ctx, id, "waiting", "", "It opens when the server starts.")
		}
	}

	for id, t := range wanted {
		if _, ok := m.tunnels[id]; ok || m.failed[id] {
			continue
		}
		starter, known := m.starters[provider]
		if !known {
			m.failed[id] = true
			m.report(ctx, id, "failed", "", "This node does not know the tunnel service "+provider+".")
			continue
		}
		// Pinggy has a free plan and QuickExpose has no accounts, the others need a token.
		if config.Token == "" && provider != PinggyProvider && provider != QuickExposeProvider {
			m.failed[id] = true
			m.report(ctx, id, "failed", "", "No tunnel token is set in the Panel.")
			continue
		}
		if provider == QuickExposeProvider && strings.TrimSpace(config.Backend) == "" {
			m.failed[id] = true
			m.report(ctx, id, "failed", "", "No backend URL is set in the Panel.")
			continue
		}

		upstream := Upstream(t.IP, t.Port)
		open, err := starter.Start(ctx, Spec{ID: id, Token: config.Token, Upstream: upstream, Backend: config.Backend})
		if err != nil {
			log.WithField("tunnel", id).WithField("error", err).Warn("could not open a tunnel")
			m.failed[id] = true
			m.report(ctx, id, "failed", "", cleanError(err))
			continue
		}
		m.tunnels[id] = &running{open: open, upstream: upstream}
		delete(m.told, id)
		m.report(ctx, id, "active", open.Address(), "")
		go m.watch(id, open)
	}
}

// watch tells the Panel when a tunnel goes away by itself, and lets the next sync open it again.
func (m *Manager) watch(id int, open Open) {
	<-open.Done()

	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.tunnels[id]; ok && r.open == open {
		delete(m.tunnels, id)
		// It is tried again at the next sync, not at once: a tunnel that keeps closing would be opened over and over.
		m.failed[id] = true
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

// Run asks the Panel what it wants now and every few minutes (that finds changes made while this node could not be reached, and tries the
// tunnels that failed again), and looks at which servers are running every few seconds.
func (m *Manager) Run(ctx context.Context) {
	if err := m.Sync(ctx); err != nil {
		log.WithField("error", err).Debug("could not ask the Panel for tunnels")
	}

	reconcile := time.NewTicker(3 * time.Second)
	defer reconcile.Stop()
	sync := time.NewTicker(3 * time.Minute)
	defer sync.Stop()

	for {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			m.closeAll()
			m.mu.Unlock()
			return
		case <-reconcile.C:
			m.Reconcile(ctx)
		case <-sync.C:
			if err := m.Sync(ctx); err != nil {
				log.WithField("error", err).Debug("could not ask the Panel for tunnels")
			}
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
