package remote

import (
	"context"
	"fmt"
)

// Tunnel is a port of this node that the Panel wants reachable from the internet (Admin > Settings > Tunnels).
type Tunnel struct {
	ID   int    `json:"id"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
	// The uuid of the server whose port it is.
	Server string `json:"server"`
}

// TunnelConfig is what the Panel wants this node to have open: the token of the tunnel service, and the tunnels.
type TunnelConfig struct {
	Provider string `json:"provider"`
	// The address of the relay, for QuickExpose.
	Backend string   `json:\"backend\"`
	Token   string   `json:"token"`
	Tunnels []Tunnel `json:"tunnels"`
}

// Tunnels asks the Panel which tunnels this node should have open.
func (c *client) Tunnels(ctx context.Context) (TunnelConfig, error) {
	var config TunnelConfig
	res, err := c.Get(ctx, "/tunnels", nil)
	if err != nil {
		return config, err
	}
	defer res.Body.Close()

	err = res.BindJSON(&config)
	return config, err
}

// ReportTunnel tells the Panel how a tunnel is doing: its state (active, failed), the public address when it is open, and why when it is not.
func (c *client) ReportTunnel(ctx context.Context, id int, state, address, message string) error {
	res, err := c.Post(ctx, fmt.Sprintf("/tunnels/%d", id), d{"state": state, "address": address, "message": message})
	if err != nil {
		return err
	}
	_ = res.Body.Close()
	return nil
}
