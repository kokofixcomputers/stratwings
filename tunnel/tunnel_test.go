package tunnel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeSource struct {
	mu      sync.Mutex
	config  Config
	reports []string
}

func (s *fakeSource) Tunnels(context.Context) (Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config, nil
}

func (s *fakeSource) ReportTunnel(_ context.Context, id int, state, address, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reports = append(s.reports, state+" "+address+" "+message)
	return nil
}

type fakeOpen struct {
	address string
	done    chan struct{}
	closed  bool
}

func (o *fakeOpen) Address() string       { return o.address }
func (o *fakeOpen) Done() <-chan struct{} { return o.done }
func (o *fakeOpen) Close() error {
	if !o.closed {
		o.closed = true
		close(o.done)
	}
	return nil
}

type fakeStarter struct {
	started []string
	tokens  []string
	fail    error
	opened  []*fakeOpen
}

func (s *fakeStarter) Start(_ context.Context, token, upstream string) (Open, error) {
	if s.fail != nil {
		return nil, s.fail
	}
	s.started = append(s.started, upstream)
	s.tokens = append(s.tokens, token)
	o := &fakeOpen{address: "0.tcp.example.io:1000" + string(rune('0'+len(s.started))), done: make(chan struct{})}
	s.opened = append(s.opened, o)
	return o, nil
}

func TestUpstream(t *testing.T) {
	cases := map[[2]any]string{
		{"0.0.0.0", 25565}:  "127.0.0.1:25565",
		{"", 25565}:         "127.0.0.1:25565",
		{"10.0.0.4", 26614}: "10.0.0.4:26614",
		{"::", 1}:           "127.0.0.1:1",
	}
	for in, want := range cases {
		if got := Upstream(in[0].(string), in[1].(int)); got != want {
			t.Errorf("Upstream(%v) = %s, want %s", in, got, want)
		}
	}
}

func TestSyncOpensWhatIsAskedForAndClosesTheRest(t *testing.T) {
	source := &fakeSource{config: Config{Token: "tok", Tunnels: []Tunnel{{ID: 1, IP: "0.0.0.0", Port: 25565}, {ID: 2, IP: "10.0.0.4", Port: 25570}}}}
	starter := &fakeStarter{}
	m := NewManager(source, map[string]Starter{NgrokProvider: starter})

	if err := m.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.Count() != 2 || len(starter.started) != 2 {
		t.Fatalf("expected two tunnels, got %d open and %d started", m.Count(), len(starter.started))
	}
	if len(source.reports) != 2 || source.reports[0][:6] != "active" {
		t.Fatalf("the Panel should have been told both are active: %v", source.reports)
	}

	// Nothing changed: nothing is opened again.
	_ = m.Sync(context.Background())
	if len(starter.started) != 2 {
		t.Fatalf("a tunnel that is open was opened again")
	}

	// The Panel does not want the second one any more.
	source.config.Tunnels = source.config.Tunnels[:1]
	_ = m.Sync(context.Background())
	if m.Count() != 1 {
		t.Fatalf("expected one tunnel left, got %d", m.Count())
	}
	closed := 0
	for _, o := range starter.opened {
		if o.closed {
			closed++
		}
	}
	if closed != 1 {
		t.Fatalf("expected one closed tunnel, got %d", closed)
	}
}

func TestANewTokenOpensEverythingAgain(t *testing.T) {
	source := &fakeSource{config: Config{Token: "one", Tunnels: []Tunnel{{ID: 1, IP: "", Port: 25565}}}}
	starter := &fakeStarter{}
	m := NewManager(source, map[string]Starter{NgrokProvider: starter})
	_ = m.Sync(context.Background())

	source.config.Token = "two"
	_ = m.Sync(context.Background())

	if len(starter.started) != 2 || starter.tokens[1] != "two" || !starter.opened[0].closed {
		t.Fatalf("the tunnel should have been opened again with the new token: %v %v", starter.started, starter.tokens)
	}
}

func TestAFailureIsReportedAndTriedAgain(t *testing.T) {
	source := &fakeSource{config: Config{Token: "tok", Tunnels: []Tunnel{{ID: 7, IP: "", Port: 25565}}}}
	starter := &fakeStarter{fail: errors.New("authentication failed: the token is not valid\nERR_NGROK_105")}
	m := NewManager(source, map[string]Starter{NgrokProvider: starter})

	_ = m.Sync(context.Background())
	if m.Count() != 0 || len(source.reports) != 1 || source.reports[0] != "failed  authentication failed: the token is not valid" {
		t.Fatalf("expected a failure with its first line: %v", source.reports)
	}

	starter.fail = nil
	_ = m.Sync(context.Background())
	if m.Count() != 1 {
		t.Fatalf("the next sync should open it")
	}
}

func TestWithoutATokenNothingIsOpened(t *testing.T) {
	source := &fakeSource{config: Config{Tunnels: []Tunnel{{ID: 1, IP: "", Port: 25565}}}}
	starter := &fakeStarter{}
	m := NewManager(source, map[string]Starter{NgrokProvider: starter})
	_ = m.Sync(context.Background())

	if m.Count() != 0 || len(starter.started) != 0 || len(source.reports) != 1 {
		t.Fatalf("no token, no tunnel: %v", source.reports)
	}
}

func TestATunnelThatGoesAwayIsReported(t *testing.T) {
	source := &fakeSource{config: Config{Token: "tok", Tunnels: []Tunnel{{ID: 1, IP: "", Port: 25565}}}}
	starter := &fakeStarter{}
	m := NewManager(source, map[string]Starter{NgrokProvider: starter})
	_ = m.Sync(context.Background())

	starter.opened[0].Close()
	// the watcher runs in its own goroutine
	for i := 0; i < 200 && m.Count() != 0; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if m.Count() != 0 {
		t.Fatalf("a closed tunnel should be forgotten")
	}
}
