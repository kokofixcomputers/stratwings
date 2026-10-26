package tunnel

import (
	"context"
	"sync"

	"golang.ngrok.com/ngrok/v2"
)

// Ngrok opens tunnels with the ngrok agent that is built into this program: nothing is installed on the node. One session is kept for each
// token, all the tunnels of the node go over it.
type Ngrok struct {
	mu     sync.Mutex
	agents map[string]ngrok.Agent
}

func NewNgrok() *Ngrok {
	return &Ngrok{agents: map[string]ngrok.Agent{}}
}

type ngrokTunnel struct {
	endpoint ngrok.EndpointForwarder
}

func (t ngrokTunnel) Address() string {
	// tcp://0.tcp.ngrok.io:12345 is told as 0.tcp.ngrok.io:12345
	return t.endpoint.URL().Host
}
func (t ngrokTunnel) Done() <-chan struct{} { return t.endpoint.Done() }
func (t ngrokTunnel) Close() error          { return t.endpoint.Close() }

func (n *Ngrok) agent(token string) (ngrok.Agent, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if agent, ok := n.agents[token]; ok {
		return agent, nil
	}

	agent, err := ngrok.NewAgent(ngrok.WithAuthtoken(token), ngrok.WithAgentDescription("Stratosphere node"))
	if err != nil {
		return nil, err
	}
	n.agents[token] = agent

	return agent, nil
}

// Start forwards a TCP address that ngrok gives out to the upstream.
func (n *Ngrok) Start(ctx context.Context, token, upstream string) (Open, error) {
	agent, err := n.agent(token)
	if err != nil {
		return nil, err
	}

	endpoint, err := agent.Forward(ctx, ngrok.WithUpstream("tcp://"+upstream), ngrok.WithURL("tcp://"))
	if err != nil {
		return nil, err
	}

	return ngrokTunnel{endpoint: endpoint}, nil
}
