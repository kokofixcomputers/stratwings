package remote

import "context"

// PrivateNetworkConfig is what the Panel wants of this node for the private network between servers.
type PrivateNetworkConfig struct {
	Enabled bool   `json:"enabled"`
	Range   string `json:"range"`
	Subnet  string `json:"subnet"`
	Gateway string `json:"gateway"`
	Port    int    `json:"port"`
	Peers   []struct {
		Name      string `json:"name"`
		Endpoint  string `json:"endpoint"`
		PublicKey string `json:"public_key"`
		Subnet    string `json:"subnet"`
	} `json:"peers"`
	Servers []struct {
		UUID    string `json:"uuid"`
		Address string `json:"address"`
		Group   int    `json:"group"`
	} `json:"servers"`
}

// PrivateNetwork asks the Panel what the private network should be on this node.
func (c *client) PrivateNetwork(ctx context.Context) (PrivateNetworkConfig, error) {
	var config PrivateNetworkConfig
	res, err := c.Get(ctx, "/private-network", nil)
	if err != nil {
		return config, err
	}
	defer res.Body.Close()

	err = res.BindJSON(&config)
	return config, err
}

// ReportPrivateNetwork tells the Panel the public key of this node (made here, nobody sets it up) and how the private network is doing.
func (c *client) ReportPrivateNetwork(ctx context.Context, publicKey, state, message string) error {
	body := d{"state": state, "message": message}
	if publicKey != "" {
		body["public_key"] = publicKey
	}
	res, err := c.Post(ctx, "/private-network", body)
	if err != nil {
		return err
	}
	_ = res.Body.Close()
	return nil
}
