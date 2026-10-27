package tunnel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"sync"
	"testing"
)

type fakeRunner struct {
	mu      sync.Mutex
	started [][]string
	stopped int
	fail    error
	done    chan struct{}
}

func (r *fakeRunner) Start(binary string, args ...string) (func(), <-chan struct{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return nil, nil, r.fail
	}
	r.started = append(r.started, append([]string{binary}, args...))
	r.done = make(chan struct{})
	return func() { r.mu.Lock(); r.stopped++; r.mu.Unlock() }, r.done, nil
}

type fakePlayitAPI struct {
	mu      sync.Mutex
	calls   []string
	bodies  map[string]map[string]any
	auth    string
	pending int
}

func (f *fakePlayitAPI) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.calls = append(f.calls, r.URL.Path)
		if f.bodies == nil {
			f.bodies = map[string]map[string]any{}
		}
		f.bodies[r.URL.Path] = body
		f.auth = r.Header.Get("Authorization")

		if f.auth != "Agent-Key good-secret" {
			_, _ = w.Write([]byte(`{"status":"error","data":{"type":"auth","message":"invalid-agent-key"}}`))
			return
		}
		switch r.URL.Path {
		case "/agents/rundata":
			_, _ = w.Write([]byte(`{"status":"success","data":{"agent_id":"11111111-1111-1111-1111-111111111111","tunnels":[],"pending":[]}}`))
		case "/tunnels/create":
			_, _ = w.Write([]byte(`{"status":"success","data":{"id":"22222222-2222-2222-2222-222222222222"}}`))
		case "/tunnels/list":
			if f.pending > 0 {
				f.pending--
				_, _ = w.Write([]byte(`{"status":"success","data":{"tunnels":[{"alloc":{"status":"pending"}}]}}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"tunnels":[{"alloc":{"status":"allocated","data":{"assigned_domain":"fancy-name.craft.ply.gg","port_start":34567}}}]}}`))
		case "/tunnels/delete":
			_, _ = w.Write([]byte(`{"status":"success","data":null}`))
		default:
			http.NotFound(w, r)
		}
	})
}

func newTestPlayit(t *testing.T, api *fakePlayitAPI, runner *fakeRunner) *Playit {
	server := httptest.NewServer(api.handler())
	t.Cleanup(server.Close)
	p := NewPlayit()
	p.api = server.URL
	p.run = runner

	return p
}

func TestPlayitMakesATunnelAndRunsTheDaemon(t *testing.T) {
	api, runner := &fakePlayitAPI{}, &fakeRunner{}
	p := newTestPlayit(t, api, runner)

	open, err := p.Start(context.Background(), Spec{Token: "good-secret", Upstream: "127.0.0.1:25565"})
	if err != nil {
		t.Fatal(err)
	}
	if open.Address() != "fancy-name.craft.ply.gg:34567" {
		t.Fatalf("address %q", open.Address())
	}
	if len(runner.started) != 1 || runner.started[0][0] != "playitd" || runner.started[0][1] != "--secret" || runner.started[0][2] != "good-secret" {
		t.Fatalf("the daemon should be started with the secret: %v", runner.started)
	}

	create := api.bodies["/tunnels/create"]
	origin := create["origin"].(map[string]any)["data"].(map[string]any)
	if create["tunnel_type"] != "minecraft-java" || create["port_type"] != "tcp" || origin["local_ip"] != "127.0.0.1" || origin["local_port"] != float64(25565) || origin["agent_id"] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("the tunnel was not asked for the right way: %v", create)
	}

	// A second tunnel uses the same daemon, which stops with the last one.
	second, err := p.Start(context.Background(), Spec{Token: "good-secret", Upstream: "127.0.0.1:25566"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runner.started) != 1 {
		t.Fatalf("the daemon should run once")
	}
	_ = open.Close()
	if runner.stopped != 0 {
		t.Fatalf("the daemon is still needed")
	}
	_ = second.Close()
	if runner.stopped != 1 {
		t.Fatalf("the daemon should stop with the last tunnel")
	}
	if api.bodies["/tunnels/delete"]["tunnel_id"] != "22222222-2222-2222-2222-222222222222" {
		t.Fatalf("the tunnel should be deleted from the account")
	}
}

func TestPlayitWaitsForTheAddress(t *testing.T) {
	api, runner := &fakePlayitAPI{pending: 2}, &fakeRunner{}
	p := newTestPlayit(t, api, runner)

	open, err := p.Start(context.Background(), Spec{Token: "good-secret", Upstream: "127.0.0.1:25565"})
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	if open.Address() == "" {
		t.Fatalf("no address")
	}
}

func TestPlayitSaysWhenTheSecretIsWrong(t *testing.T) {
	api, runner := &fakePlayitAPI{}, &fakeRunner{}
	p := newTestPlayit(t, api, runner)

	_, err := p.Start(context.Background(), Spec{Token: "wrong", Upstream: "127.0.0.1:25565"})
	if err == nil || err.Error() != "playit.gg refused: auth invalid-agent-key (is the agent secret key right?)" {
		t.Fatalf("got %v", err)
	}
	if runner.stopped != 1 {
		t.Fatalf("the daemon is not needed when no tunnel could be made")
	}
}

func TestPlayitSaysWhenTheDaemonIsNotInstalled(t *testing.T) {
	api, runner := &fakePlayitAPI{}, &fakeRunner{fail: exec.ErrNotFound}
	p := newTestPlayit(t, api, runner)

	_, err := p.Start(context.Background(), Spec{Token: "good-secret", Upstream: "127.0.0.1:25565"})
	if err == nil {
		t.Fatalf("an error was expected")
	}
	if err.Error() != "The playit agent is not installed on this node: install the playit package, which has playitd." {
		t.Fatalf("got %q", err.Error())
	}
	if len(api.calls) != 0 {
		t.Fatalf("the API should not be asked without the daemon")
	}
}

func TestATunnelIsGoneWhenTheDaemonIs(t *testing.T) {
	api, runner := &fakePlayitAPI{}, &fakeRunner{}
	p := newTestPlayit(t, api, runner)
	open, err := p.Start(context.Background(), Spec{Token: "good-secret", Upstream: "127.0.0.1:25565"})
	if err != nil {
		t.Fatal(err)
	}

	close(runner.done)
	<-open.Done()
}
