// Package privnet is the private network between the servers of a person (Admin > Settings > Private network in the Panel). The Panel gives
// every node a slice of a range of private addresses (10.99.3.0/24) and every server that joined an address in the slice of its node; this
// node makes the encrypted tunnel (WireGuard) to the other nodes, with a key it makes itself, a Docker network for the slice, attaches the
// containers with their address and keeps the servers of other people out. Nobody sets up keys or addresses by hand.
package privnet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apex/log"
)

// Peer is another node of the private network.
type Peer struct {
	Name      string
	Endpoint  string
	PublicKey string
	Subnet    string
}

// Member is a server that joined, with its owner: the servers of one owner (the group) can reach each other.
type Member struct {
	UUID    string
	Address string
	Group   int
}

// Config is what the Panel wants of this node.
type Config struct {
	Enabled bool
	// The whole range, and the slice and gateway (the node itself) of this node.
	Range   string
	Subnet  string
	Gateway string
	// The UDP port of the tunnel.
	Port    int
	Peers   []Peer
	Servers []Member
}

// Report is what this node says about itself.
type Report struct {
	PublicKey string
	// ready, error or off.
	State   string
	Message string
}

// Source is the Panel, as far as the private network goes.
type Source interface {
	PrivateNetwork(ctx context.Context) (Config, error)
	ReportPrivateNetwork(ctx context.Context, report Report) error
}

// Attached is a container that is on the private network.
type Attached struct {
	Container string
	Address   string
}

// WireSpec is the tunnel: the key of this node, the port, and the other nodes with the slice each one is reached for.
type WireSpec struct {
	Port  int
	Peers []Peer
}

// NetSpec is the Docker network: the whole range is its subnet (so containers send everything for the range to it), and the addresses are given out
// of the slice of this node.
type NetSpec struct {
	Range   string
	Slice   string
	Gateway string
}

// Platform is everything that touches the machine. The real one is in linux.go, tests put their own in.
type Platform interface {
	// PublicKey is the key of this node: made the first time, kept after.
	PublicKey() (string, error)
	EnsureWire(spec WireSpec) error
	RemoveWire() error
	// EnsureNetwork makes the Docker network (made again when its range changed) and says the name of its bridge on the host.
	EnsureNetwork(spec NetSpec) (bridge string, err error)
	RemoveNetwork() error
	// EnsureHost sets what the host needs to pass traffic between the bridge and the tunnel.
	EnsureHost(bridge string, port int) error
	ApplyFirewall(rules []string) error
	RemoveFirewall() error
	// Attached lists the containers on the private network.
	Attached() ([]Attached, error)
	// Exists says whether a container with this name is there.
	Exists(container string) (bool, error)
	Attach(container, address string) error
	Detach(container string) error
}

// Manager keeps this node what the Panel wants.
type Manager struct {
	source   Source
	platform Platform

	mu     sync.Mutex
	config Config
	// Whether the network is up, and what was reported last (a report is only sent again when something changes, and every few minutes).
	up       bool
	reported Report
	reportAt time.Time
	bridge   string
	spec     NetSpec
}

// Default is the manager of this node, set when it starts. The Panel asks it to look again when something changed.
var Default *Manager

func NewManager(source Source, platform Platform) *Manager {
	return &Manager{source: source, platform: platform}
}

// Sync asks the Panel what it wants and makes the node so.
func (m *Manager) Sync(ctx context.Context) error {
	config, err := m.source.PrivateNetwork(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = config

	report := m.apply(config)
	m.tell(ctx, report)

	return nil
}

// Reconcile attaches and detaches containers with what the Panel said last: a container that was just made has to be put on the network,
// the container of a server that left taken off it.
func (m *Manager) Reconcile(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.up {
		return
	}
	if err := m.attach(m.config); err != nil {
		log.WithField("error", err).Debug("private network: could not attach a container")
	}
}

// Wire is the key for a report: it does not need the tunnel to be up.
func (m *Manager) key() string {
	key, err := m.platform.PublicKey()
	if err != nil {
		return ""
	}

	return key
}

func (m *Manager) tell(ctx context.Context, report Report) {
	if report == m.reported && time.Since(m.reportAt) < 3*time.Minute {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m.source.ReportPrivateNetwork(ctx, report); err != nil {
		log.WithField("error", err).Debug("private network: could not tell the Panel")
		return
	}
	m.reported, m.reportAt = report, time.Now()
}

// apply must be called with the lock held. It makes what the Panel wants and says how it went.
func (m *Manager) apply(config Config) Report {
	if !config.Enabled {
		return m.teardown()
	}

	key, err := m.platform.PublicKey()
	if err != nil {
		return Report{State: "error", Message: "could not make the key of this node: " + err.Error()}
	}
	report := Report{PublicKey: key, State: "ready"}
	fail := func(what string, err error) Report {
		report.State = "error"
		report.Message = what + ": " + err.Error()
		return report
	}

	spec := NetSpec{Range: config.Range, Slice: config.Subnet, Gateway: config.Gateway}
	// The network is made again when its range or slice changed, so what is on it is attached again with the new addresses.
	if m.up && spec != m.spec {
		_ = m.platform.RemoveNetwork()
		m.up = false
	}
	bridge, err := m.platform.EnsureNetwork(spec)
	if err != nil {
		return fail("could not make the Docker network", err)
	}
	m.spec, m.bridge = spec, bridge

	if err := m.platform.EnsureWire(WireSpec{Port: config.Port, Peers: config.Peers}); err != nil {
		return fail("could not set up the tunnel (does the kernel have WireGuard?)", err)
	}
	if err := m.platform.EnsureHost(bridge, config.Port); err != nil {
		return fail("could not set up the host", err)
	}
	if err := m.platform.ApplyFirewall(Rules(config)); err != nil {
		return fail("could not set up the firewall", err)
	}
	m.up = true

	if err := m.attach(config); err != nil {
		return fail("could not attach a container", err)
	}

	return report
}

// teardown takes everything away, for a node that is not on the private network (any more).
func (m *Manager) teardown() Report {
	if m.up {
		_ = m.platform.RemoveFirewall()
		_ = m.platform.RemoveNetwork()
		_ = m.platform.RemoveWire()
		m.up = false
	}

	return Report{PublicKey: m.key(), State: "off"}
}

// attach puts the containers of the servers that joined on the network with their address, and takes the others off it.
func (m *Manager) attach(config Config) error {
	have, err := m.platform.Attached()
	if err != nil {
		return err
	}
	now := map[string]string{}
	for _, a := range have {
		now[a.Container] = a.Address
	}

	var failed []string
	for _, member := range config.Servers {
		// Only the servers of this node have a container here, and only the ones with a slice address of this node are asked for.
		if !Within(config.Subnet, member.Address) {
			continue
		}
		address, attached := now[member.UUID]
		delete(now, member.UUID)
		if attached && address == member.Address {
			continue
		}
		exists, err := m.platform.Exists(member.UUID)
		if err != nil || !exists {
			continue
		}
		if attached {
			_ = m.platform.Detach(member.UUID)
		}
		if err := m.platform.Attach(member.UUID, member.Address); err != nil {
			failed = append(failed, member.UUID+": "+err.Error())
		}
	}

	// What is on the network but not wanted any more.
	extra := make([]string, 0, len(now))
	for container := range now {
		extra = append(extra, container)
	}
	sort.Strings(extra)
	for _, container := range extra {
		_ = m.platform.Detach(container)
	}

	if len(failed) > 0 {
		return fmt.Errorf("%s", strings.Join(failed, "; "))
	}

	return nil
}

// Run syncs now and every minute (what changed while this node could not be reached, a new key of another node), and looks at the
// containers every few seconds.
func (m *Manager) Run(ctx context.Context) {
	if err := m.Sync(ctx); err != nil {
		log.WithField("error", err).Debug("private network: could not ask the Panel")
	}

	reconcile := time.NewTicker(3 * time.Second)
	defer reconcile.Stop()
	sync := time.NewTicker(time.Minute)
	defer sync.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-reconcile.C:
			m.Reconcile(ctx)
		case <-sync.C:
			if err := m.Sync(ctx); err != nil {
				log.WithField("error", err).Debug("private network: could not ask the Panel")
			}
		}
	}
}
