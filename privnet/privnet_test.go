package privnet

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type fakeSource struct {
	config  Config
	reports []Report
}

func (s *fakeSource) PrivateNetwork(context.Context) (Config, error) { return s.config, nil }
func (s *fakeSource) ReportPrivateNetwork(_ context.Context, r Report) error {
	s.reports = append(s.reports, r)
	return nil
}

type fakePlatform struct {
	wire      []WireSpec
	network   []NetSpec
	host      []string
	firewall  [][]string
	removed   []string
	attached  map[string]string
	exists    map[string]bool
	attachLog []string
	fail      map[string]error
}

func newFake() *fakePlatform {
	return &fakePlatform{attached: map[string]string{}, exists: map[string]bool{}, fail: map[string]error{}}
}

func (p *fakePlatform) PublicKey() (string, error) { return "PUBLICKEY=", p.fail["key"] }
func (p *fakePlatform) EnsureWire(s WireSpec) error {
	p.wire = append(p.wire, s)
	return p.fail["wire"]
}
func (p *fakePlatform) RemoveWire() error { p.removed = append(p.removed, "wire"); return nil }
func (p *fakePlatform) EnsureNetwork(s NetSpec) (string, error) {
	p.network = append(p.network, s)
	return "strat_priv0", p.fail["network"]
}
func (p *fakePlatform) RemoveNetwork() error {
	p.removed = append(p.removed, "network")
	p.attached = map[string]string{}
	return nil
}
func (p *fakePlatform) EnsureHost(bridge string, port int) error {
	p.host = append(p.host, bridge)
	return p.fail["host"]
}
func (p *fakePlatform) ApplyFirewall(rules []string) error {
	p.firewall = append(p.firewall, rules)
	return p.fail["firewall"]
}
func (p *fakePlatform) RemoveFirewall() error { p.removed = append(p.removed, "firewall"); return nil }
func (p *fakePlatform) Attached() ([]Attached, error) {
	var out []Attached
	for c, a := range p.attached {
		out = append(out, Attached{c, a})
	}
	return out, nil
}
func (p *fakePlatform) Exists(c string) (bool, error) { return p.exists[c], nil }
func (p *fakePlatform) Attach(c, a string) error {
	p.attachLog = append(p.attachLog, "attach "+c+" "+a)
	if err := p.fail["attach"]; err != nil {
		return err
	}
	p.attached[c] = a
	return nil
}
func (p *fakePlatform) Detach(c string) error {
	p.attachLog = append(p.attachLog, "detach "+c)
	delete(p.attached, c)
	return nil
}

func baseConfig() Config {
	return Config{
		Enabled: true, Range: "10.99.0.0/16", Subnet: "10.99.3.0/24", Gateway: "10.99.3.1", Port: 51820,
		Peers: []Peer{{Name: "b", Endpoint: "b.example.com:51820", PublicKey: "KEYB=", Subnet: "10.99.5.0/24"}},
		Servers: []Member{
			{UUID: "u1", Address: "10.99.3.2", Group: 1},
			{UUID: "u2", Address: "10.99.3.3", Group: 1},
			{UUID: "u3", Address: "10.99.3.4", Group: 2},
			{UUID: "u4", Address: "10.99.5.2", Group: 1},
			{UUID: "u5", Address: "10.99.5.3", Group: 2},
		},
	}
}

func TestWithin(t *testing.T) {
	if !Within("10.99.3.0/24", "10.99.3.7") || Within("10.99.3.0/24", "10.99.4.7") || Within("nonsense", "10.99.3.7") || Within("10.99.3.0/24", "nope") {
		t.Fatal("Within is wrong")
	}
}

func TestRulesLetTheServersOfAnOwnerReachEachOtherAndNobodyElse(t *testing.T) {
	got := Rules(baseConfig())
	want := []string{
		"-m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT",
		// u1 and u2 are of owner 1, who also has u4 on the other node: they reach each other, and not the ones of owner 2.
		"-d 10.99.3.2 -s 10.99.3.3,10.99.5.2 -j ACCEPT",
		"-d 10.99.3.3 -s 10.99.3.2,10.99.5.2 -j ACCEPT",
		// u3 is of owner 2, who has u5 on the other node.
		"-d 10.99.3.4 -s 10.99.5.3 -j ACCEPT",
		"-d 10.99.0.0/16 -j DROP",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rules:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestRulesForAServerAloneHaveNoAllowRule(t *testing.T) {
	config := baseConfig()
	config.Servers = []Member{{UUID: "u1", Address: "10.99.3.2", Group: 1}}
	got := Rules(config)
	if len(got) != 2 || got[1] != "-d 10.99.0.0/16 -j DROP" {
		t.Fatalf("a server with nobody to reach is reached by nobody: %v", got)
	}
}

func TestSyncSetsEverythingUpAndSaysItIsReady(t *testing.T) {
	source := &fakeSource{config: baseConfig()}
	platform := newFake()
	platform.exists = map[string]bool{"u1": true, "u2": true, "u3": true}
	m := NewManager(source, platform)

	if err := m.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(platform.wire) != 1 || platform.wire[0].Port != 51820 || len(platform.wire[0].Peers) != 1 {
		t.Fatalf("the tunnel should be made with the peers: %+v", platform.wire)
	}
	if !reflect.DeepEqual(platform.network, []NetSpec{{Range: "10.99.0.0/16", Slice: "10.99.3.0/24", Gateway: "10.99.3.1"}}) {
		t.Fatalf("network %+v", platform.network)
	}
	if len(platform.firewall) != 1 || len(platform.host) != 1 || platform.host[0] != "strat_priv0" {
		t.Fatalf("host and firewall: %v %v", platform.host, platform.firewall)
	}
	// Only the servers of this node (the addresses of its slice) that have a container are attached.
	got := append([]string{}, platform.attachLog...)
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"attach u1 10.99.3.2", "attach u2 10.99.3.3", "attach u3 10.99.3.4"}) {
		t.Fatalf("attached %v", got)
	}
	if len(source.reports) != 1 || source.reports[0] != (Report{PublicKey: "PUBLICKEY=", State: "ready"}) {
		t.Fatalf("report %+v", source.reports)
	}
}

func TestAServerThatJoinsLaterAndOneThatLeavesAreFollowed(t *testing.T) {
	source := &fakeSource{config: baseConfig()}
	platform := newFake()
	platform.exists = map[string]bool{"u1": true}
	m := NewManager(source, platform)
	_ = m.Sync(context.Background())

	// A container that did not exist yet is made: the next look attaches it.
	platform.exists["u2"] = true
	m.Reconcile(context.Background())
	if platform.attached["u2"] != "10.99.3.3" {
		t.Fatalf("u2 should be attached when its container is there: %v", platform.attached)
	}
	before := len(platform.attachLog)
	m.Reconcile(context.Background())
	if len(platform.attachLog) != before {
		t.Fatalf("nothing changed, nothing should be done")
	}

	// u1 leaves (and so does its rule).
	source.config.Servers = source.config.Servers[1:]
	_ = m.Sync(context.Background())
	if _, still := platform.attached["u1"]; still {
		t.Fatalf("a server that left is taken off the network")
	}
	last := platform.firewall[len(platform.firewall)-1]
	for _, rule := range last {
		if strings.Contains(rule, "10.99.3.2 ") && strings.HasPrefix(rule, "-d 10.99.3.2") {
			t.Fatalf("a server that left has no rule: %v", last)
		}
	}
}

func TestAnAddressThatChangedIsAttachedAgain(t *testing.T) {
	source := &fakeSource{config: baseConfig()}
	platform := newFake()
	platform.exists = map[string]bool{"u1": true}
	m := NewManager(source, platform)
	_ = m.Sync(context.Background())
	platform.attached["u1"] = "10.99.3.99"

	m.Reconcile(context.Background())
	if platform.attached["u1"] != "10.99.3.2" || platform.attachLog[len(platform.attachLog)-2] != "detach u1" {
		t.Fatalf("the address is put right: %v %v", platform.attached, platform.attachLog)
	}
}

func TestANewRangeMakesTheNetworkAgain(t *testing.T) {
	source := &fakeSource{config: baseConfig()}
	platform := newFake()
	platform.exists = map[string]bool{"u1": true}
	m := NewManager(source, platform)
	_ = m.Sync(context.Background())

	source.config.Range = "10.61.0.0/16"
	source.config.Subnet = "10.61.1.0/24"
	source.config.Gateway = "10.61.1.1"
	source.config.Servers = []Member{{UUID: "u1", Address: "10.61.1.2", Group: 1}}
	_ = m.Sync(context.Background())

	if !contains(platform.removed, "network") || len(platform.network) != 2 || platform.attached["u1"] != "10.61.1.2" {
		t.Fatalf("a new range is a new network: removed %v networks %v attached %v", platform.removed, platform.network, platform.attached)
	}
}

func contains(list []string, item string) bool {
	for _, i := range list {
		if i == item {
			return true
		}
	}
	return false
}

func TestWhenTheNodeIsTakenOutEverythingGoes(t *testing.T) {
	source := &fakeSource{config: baseConfig()}
	platform := newFake()
	m := NewManager(source, platform)
	_ = m.Sync(context.Background())

	source.config = Config{Enabled: false}
	_ = m.Sync(context.Background())

	if !contains(platform.removed, "wire") || !contains(platform.removed, "network") || !contains(platform.removed, "firewall") {
		t.Fatalf("everything should be removed: %v", platform.removed)
	}
	if last := source.reports[len(source.reports)-1]; last.State != "off" {
		t.Fatalf("the Panel is told it is off: %+v", last)
	}
	// Nothing is attached to a network that is not there.
	m.Reconcile(context.Background())
}

func TestFailuresAreToldWithWhatWentWrong(t *testing.T) {
	cases := map[string]string{
		"network":  "could not make the Docker network",
		"wire":     "could not set up the tunnel",
		"host":     "could not set up the host",
		"firewall": "could not set up the firewall",
	}
	for step, want := range cases {
		source := &fakeSource{config: baseConfig()}
		platform := newFake()
		platform.fail[step] = errors.New("boom")
		m := NewManager(source, platform)
		_ = m.Sync(context.Background())

		report := source.reports[len(source.reports)-1]
		if report.State != "error" || !strings.Contains(report.Message, want) || !strings.Contains(report.Message, "boom") || report.PublicKey != "PUBLICKEY=" {
			t.Errorf("%s: report %+v", step, report)
		}
	}
}

func TestTheKeyIsReportedEvenWhenOff(t *testing.T) {
	source := &fakeSource{config: Config{Enabled: false}}
	m := NewManager(source, newFake())
	_ = m.Sync(context.Background())
	if r := source.reports[0]; r.State != "off" || r.PublicKey != "PUBLICKEY=" {
		t.Fatalf("%+v", r)
	}
}

func TestAReportIsNotRepeatedUntilSomethingChanges(t *testing.T) {
	source := &fakeSource{config: baseConfig()}
	m := NewManager(source, newFake())
	_ = m.Sync(context.Background())
	_ = m.Sync(context.Background())
	if len(source.reports) != 1 {
		t.Fatalf("the same report is not sent again: %d", len(source.reports))
	}
}
