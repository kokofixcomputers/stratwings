package tunnel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Playit opens tunnels with playit.gg. Two parts: the tunnels are made on the account through its API (a minecraft-java tunnel that leads to
// the port on this node), and the playit daemon (`playitd`, from the playit package) has to run on the node with the secret key of the agent,
// which is what carries the traffic. Wings starts it when the first tunnel is opened and stops it when the last one is closed.
type Playit struct {
	api    string
	http   *http.Client
	daemon string
	run    runner

	mu     sync.Mutex
	agents map[string]*playitAgent
}

// runner starts the daemon; tests put their own in.
type runner interface {
	Start(binary string, args ...string) (stop func(), done <-chan struct{}, err error)
}

type execRunner struct{}

func (execRunner) Start(binary string, args ...string) (func(), <-chan struct{}, error) {
	cmd := exec.Command(binary, args...)
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	return func() { _ = cmd.Process.Kill() }, done, nil
}

type playitAgent struct {
	stop    func()
	done    <-chan struct{}
	tunnels int
}

func NewPlayit() *Playit {
	return &Playit{
		api:    "https://api.playit.gg",
		http:   &http.Client{Timeout: 20 * time.Second},
		daemon: "playitd",
		run:    execRunner{},
		agents: map[string]*playitAgent{},
	}
}

type playitTunnel struct {
	p       *Playit
	token   string
	id      string
	address string
	agent   *playitAgent
	done    chan struct{}
	once    sync.Once
}

func (t *playitTunnel) Address() string       { return t.address }
func (t *playitTunnel) Done() <-chan struct{} { return t.done }

func (t *playitTunnel) Close() error {
	var err error
	t.once.Do(func() {
		close(t.done)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		err = t.p.call(ctx, t.token, "/tunnels/delete", map[string]any{"tunnel_id": t.id}, nil)
		t.p.release(t.token, t.agent)
	})

	return err
}

// call makes a request of the API of playit and unwraps its answer ({"status":"success","data":...}).
func (p *Playit) call(ctx context.Context, token, path string, body any, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.api+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Agent-Key "+strings.TrimSpace(token))

	res, err := p.http.Do(req)
	if err != nil {
		return fmt.Errorf("could not reach playit.gg: %w", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))

	var envelope struct {
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("playit.gg answered %d with something that is not understood", res.StatusCode)
	}
	if envelope.Status != "success" {
		return fmt.Errorf("playit.gg refused: %s", playitReason(envelope.Status, envelope.Data))
	}
	if out != nil {
		return json.Unmarshal(envelope.Data, out)
	}
	return nil
}

// playitReason is what playit.gg said, as a short text: its error has a type and sometimes a message.
func playitReason(status string, data json.RawMessage) string {
	var detail struct {
		Type    string          `json:"type"`
		Message json.RawMessage `json:"message"`
	}
	if json.Unmarshal(data, &detail) == nil && detail.Type != "" {
		reason := detail.Type
		if len(detail.Message) > 0 && string(detail.Message) != "null" {
			reason += " " + strings.Trim(string(detail.Message), `"`)
		}
		if detail.Type == "auth" || strings.Contains(reason, "invalid-agent-key") || strings.Contains(reason, "unauthorized") {
			reason += " (is the agent secret key right?)"
		}
		return reason
	}
	if status == "" {
		status = "an error"
	}
	return status
}

// start makes sure the daemon of the agent with this key is running, counting this tunnel as one that uses it.
func (p *Playit) start(token string) (*playitAgent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if agent, ok := p.agents[token]; ok {
		select {
		case <-agent.done:
			delete(p.agents, token)
		default:
			agent.tunnels++
			return agent, nil
		}
	}

	stop, done, err := p.run.Start(p.daemon, "--secret", strings.TrimSpace(token))
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return nil, errors.New("The playit agent is not installed on this node: install the playit package, which has playitd.")
		}
		return nil, fmt.Errorf("could not start playitd: %w", err)
	}
	agent := &playitAgent{stop: stop, done: done, tunnels: 1}
	p.agents[token] = agent

	return agent, nil
}

// release says that a tunnel stopped using the daemon, which is stopped with the last one.
func (p *Playit) release(token string, agent *playitAgent) {
	p.mu.Lock()
	defer p.mu.Unlock()

	agent.tunnels--
	if agent.tunnels <= 0 {
		agent.stop()
		if p.agents[token] == agent {
			delete(p.agents, token)
		}
	}
}

// Start makes a tunnel on the account that leads to the upstream, and waits until playit.gg has given it an address.
func (p *Playit) Start(ctx context.Context, spec Spec) (Open, error) {
	token := spec.Token
	host, portText, err := net.SplitHostPort(spec.Upstream)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return nil, err
	}

	agent, err := p.start(token)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (Open, error) {
		p.release(token, agent)
		return nil, err
	}

	var run struct {
		AgentID string `json:"agent_id"`
	}
	if err := p.call(ctx, token, "/agents/rundata", map[string]any{}, &run); err != nil {
		return fail(err)
	}

	var created struct {
		ID string `json:"id"`
	}
	err = p.call(ctx, token, "/tunnels/create", map[string]any{
		"name":           "stratosphere-" + portText,
		"tunnel_type":    "minecraft-java",
		"port_type":      "tcp",
		"port_count":     1,
		"enabled":        true,
		"alloc":          nil,
		"firewall_id":    nil,
		"proxy_protocol": nil,
		"origin": map[string]any{
			"type": "agent",
			"data": map[string]any{"agent_id": run.AgentID, "local_ip": host, "local_port": port},
		},
	}, &created)
	if err != nil {
		return fail(err)
	}

	tunnel := &playitTunnel{p: p, token: token, id: created.ID, agent: agent, done: make(chan struct{})}
	address, err := p.waitForAddress(ctx, token, created.ID)
	if err != nil {
		// The tunnel that was made is taken away again, it would be left on the account otherwise.
		_ = p.call(context.Background(), token, "/tunnels/delete", map[string]any{"tunnel_id": created.ID}, nil)
		return fail(err)
	}
	tunnel.address = address

	// The tunnel is gone when the daemon is.
	go func() {
		select {
		case <-agent.done:
			tunnel.once.Do(func() { close(tunnel.done) })
		case <-tunnel.done:
		}
	}()

	return tunnel, nil
}

func (p *Playit) waitForAddress(ctx context.Context, token, id string) (string, error) {
	deadline := time.Now().Add(45 * time.Second)
	for {
		var list struct {
			Tunnels []struct {
				Alloc struct {
					Status string `json:"status"`
					Data   struct {
						AssignedDomain string `json:"assigned_domain"`
						IPHostname     string `json:"ip_hostname"`
						PortStart      int    `json:"port_start"`
					} `json:"data"`
				} `json:"alloc"`
				DisabledReason json.RawMessage `json:"disabled_reason"`
			} `json:"tunnels"`
		}
		if err := p.call(ctx, token, "/tunnels/list", map[string]any{"tunnel_id": id, "agent_id": nil}, &list); err != nil {
			return "", err
		}
		if len(list.Tunnels) > 0 {
			alloc := list.Tunnels[0].Alloc
			if alloc.Status == "allocated" {
				host := alloc.Data.AssignedDomain
				if host == "" {
					host = alloc.Data.IPHostname
				}
				return net.JoinHostPort(host, strconv.Itoa(alloc.Data.PortStart)), nil
			}
			if alloc.Status == "disabled" {
				return "", fmt.Errorf("playit.gg turned the tunnel off: %s", strings.Trim(string(list.Tunnels[0].DisabledReason), `"{}`))
			}
		}
		if time.Now().After(deadline) {
			return "", errors.New("playit.gg did not give the tunnel an address in time")
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
