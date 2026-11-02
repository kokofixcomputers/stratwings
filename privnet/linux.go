//go:build linux

package privnet

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/apex/log"
	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/vishvananda/netlink"
	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	"github.com/pterodactyl/wings/environment"
)

const (
	// The names on the machine: the tunnel interface, the Docker network, and its bridge.
	wireName    = "strat0"
	networkName = "stratosphere_private"
	bridgeName  = "strat_priv0"
	// The tunnel has 60 bytes of headers, and what is in it is sent over a network that has 1500.
	wireMTU   = 1420
	bridgeMTU = "1400"
)

// Linux is the Platform of a node: WireGuard of the kernel for the tunnel (with the library, no `wg` tool is needed), the Docker API for
// the network and the containers, and iptables (which Docker needs anyway) for the rules.
type Linux struct {
	dir string
}

func NewLinux(dir string) *Linux { return &Linux{dir: dir} }

func (l *Linux) docker() (*client.Client, error) { return environment.Docker() }

// ---- the key ---------------------------------------------------------------------------------------------------------

func (l *Linux) privateKey() (wgtypes.Key, error) {
	path := filepath.Join(l.dir, "wg.key")
	if raw, err := os.ReadFile(path); err == nil {
		return wgtypes.ParseKey(strings.TrimSpace(string(raw)))
	}
	key, err := wgtypes.GeneratePrivateKey()
	if err != nil {
		return wgtypes.Key{}, err
	}
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		return wgtypes.Key{}, err
	}
	if err := os.WriteFile(path, []byte(key.String()+"\n"), 0o600); err != nil {
		return wgtypes.Key{}, err
	}

	return key, nil
}

func (l *Linux) PublicKey() (string, error) {
	key, err := l.privateKey()
	if err != nil {
		return "", err
	}

	return key.PublicKey().String(), nil
}

// ---- the tunnel ------------------------------------------------------------------------------------------------------

func (l *Linux) EnsureWire(spec WireSpec) error {
	key, err := l.privateKey()
	if err != nil {
		return err
	}

	link, err := netlink.LinkByName(wireName)
	if err != nil {
		if _, ok := err.(netlink.LinkNotFoundError); !ok {
			return err
		}
		link = &netlink.Wireguard{LinkAttrs: netlink.LinkAttrs{Name: wireName, MTU: wireMTU}}
		if err := netlink.LinkAdd(link); err != nil {
			return fmt.Errorf("could not make the interface %s: %w", wireName, err)
		}
		if link, err = netlink.LinkByName(wireName); err != nil {
			return err
		}
	}
	if err := netlink.LinkSetUp(link); err != nil {
		return err
	}

	keepalive := 25 * time.Second
	peers := make([]wgtypes.PeerConfig, 0, len(spec.Peers))
	var routes []*net.IPNet
	for _, peer := range spec.Peers {
		public, err := wgtypes.ParseKey(peer.PublicKey)
		if err != nil {
			log.WithField("peer", peer.Name).WithField("error", err).Warn("private network: the key of a node is not a key, it is left out")
			continue
		}
		// The address of the node is looked up now, a name that changed address is followed at the next sync.
		endpoint, err := net.ResolveUDPAddr("udp", peer.Endpoint)
		if err != nil {
			log.WithField("peer", peer.Name).WithField("error", err).Warn("private network: could not find the address of a node, it is left out")
			continue
		}
		_, subnet, err := net.ParseCIDR(peer.Subnet)
		if err != nil {
			continue
		}
		peers = append(peers, wgtypes.PeerConfig{
			PublicKey:                   public,
			Endpoint:                    endpoint,
			PersistentKeepaliveInterval: &keepalive,
			ReplaceAllowedIPs:           true,
			AllowedIPs:                  []net.IPNet{*subnet},
		})
		routes = append(routes, subnet)
	}

	wg, err := wgctrl.New()
	if err != nil {
		return fmt.Errorf("WireGuard is not available: %w", err)
	}
	defer wg.Close()
	port := spec.Port
	if err := wg.ConfigureDevice(wireName, wgtypes.Config{PrivateKey: &key, ListenPort: &port, ReplacePeers: true, Peers: peers}); err != nil {
		return err
	}

	// What is for the slice of another node goes into the tunnel, and what is not wanted any more is dropped.
	want := map[string]bool{}
	for _, subnet := range routes {
		want[subnet.String()] = true
		if err := netlink.RouteReplace(&netlink.Route{LinkIndex: link.Attrs().Index, Dst: subnet, Scope: netlink.SCOPE_LINK}); err != nil {
			return fmt.Errorf("could not route %s: %w", subnet, err)
		}
	}
	existing, err := netlink.RouteList(link, netlink.FAMILY_V4)
	if err != nil {
		return err
	}
	for i := range existing {
		if existing[i].Dst != nil && !want[existing[i].Dst.String()] {
			_ = netlink.RouteDel(&existing[i])
		}
	}

	return nil
}

func (l *Linux) RemoveWire() error {
	link, err := netlink.LinkByName(wireName)
	if err != nil {
		return nil
	}

	return netlink.LinkDel(link)
}

// ---- the Docker network ----------------------------------------------------------------------------------------------

func (l *Linux) EnsureNetwork(spec NetSpec) (string, error) {
	cli, err := l.docker()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	existing, err := cli.NetworkInspect(ctx, networkName, network.InspectOptions{})
	if err == nil {
		same := len(existing.IPAM.Config) == 1 && existing.IPAM.Config[0].Subnet == spec.Range &&
			existing.IPAM.Config[0].IPRange == spec.Slice && existing.IPAM.Config[0].Gateway == spec.Gateway
		if same {
			return bridgeName, nil
		}
		// Another range or slice: the network is made again, the containers are put back by the next look.
		if err := l.RemoveNetwork(); err != nil {
			return "", err
		}
	} else if !cerrdefs.IsNotFound(err) {
		return "", err
	}

	enableIPv6 := false
	_, err = cli.NetworkCreate(ctx, networkName, network.CreateOptions{
		Driver:     "bridge",
		EnableIPv6: &enableIPv6,
		IPAM: &network.IPAM{Driver: "default", Config: []network.IPAMConfig{
			// The subnet is the whole range, so a container sends everything for the range to the bridge; the addresses it gets are the slice.
			{Subnet: spec.Range, IPRange: spec.Slice, Gateway: spec.Gateway},
		}},
		Options: map[string]string{
			"com.docker.network.bridge.name": bridgeName,
			// The addresses are kept as they are: no NAT between the nodes.
			"com.docker.network.bridge.enable_ip_masquerade": "false",
			"com.docker.network.driver.mtu":                  bridgeMTU,
		},
		Labels: map[string]string{"Service": "Pterodactyl", "ContainerType": "private_network"},
	})
	if err != nil {
		return "", fmt.Errorf("could not make the network %s: %w", networkName, err)
	}

	return bridgeName, nil
}

func (l *Linux) RemoveNetwork() error {
	cli, err := l.docker()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	existing, err := cli.NetworkInspect(ctx, networkName, network.InspectOptions{})
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	for id := range existing.Containers {
		_ = cli.NetworkDisconnect(ctx, networkName, id, true)
	}

	return cli.NetworkRemove(ctx, networkName)
}

// ---- the host and the rules ---------------------------------------------------------------------------------------

func writeSysctl(path, value string) error {
	return os.WriteFile(path, []byte(value), 0o644)
}

func (l *Linux) EnsureHost(bridge string, port int, tunnel bool) error {
	// The node passes traffic between the bridge and the tunnel.
	if err := writeSysctl("/proc/sys/net/ipv4/ip_forward", "1"); err != nil {
		return err
	}
	// A container asks (ARP) for the address of a server on another node as if it was on the bridge: the node answers for it, because it knows the
	// way into the tunnel.
	if err := writeSysctl("/proc/sys/net/ipv4/conf/"+bridge+"/proxy_arp", "1"); err != nil {
		return err
	}
	// Traffic between containers of the same bridge goes through the rules too (Docker turns this on, the module may not be loaded yet).
	_ = writeSysctl("/proc/sys/net/bridge/bridge-nf-call-iptables", "1")

	if !tunnel {
		return nil
	}

	// The tunnel port is let in. A firewall that is not iptables has to be told by whoever runs the node.
	rule := []string{"INPUT", "-p", "udp", "--dport", fmt.Sprint(port), "-j", "ACCEPT"}
	if iptables(append([]string{"-C"}, rule...)...) != nil {
		if err := iptables(append([]string{"-I", "INPUT", "1"}, rule[1:]...)...); err != nil {
			log.WithField("error", err).Warn("private network: could not open the tunnel port in iptables, open UDP " + fmt.Sprint(port) + " in the firewall")
		}
	}

	return nil
}

func iptables(args ...string) error {
	cmd := exec.Command("iptables", append([]string{"-w", "5"}, args...)...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("iptables %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(out.String()))
	}

	return nil
}

// parent is the chain that has the jump to ours: DOCKER-USER, which Docker looks at before its own rules, or FORWARD where there is none.
func parentChain() string {
	if iptables("-L", "DOCKER-USER", "-n") == nil {
		return "DOCKER-USER"
	}

	return "FORWARD"
}

func (l *Linux) ApplyFirewall(rules []string) error {
	if err := iptables("-N", Chain); err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	if err := iptables("-F", Chain); err != nil {
		return err
	}
	for _, rule := range rules {
		if err := iptables(append([]string{"-A", Chain}, strings.Fields(rule)...)...); err != nil {
			return err
		}
	}
	if err := iptables("-A", Chain, "-j", "RETURN"); err != nil {
		return err
	}

	parent := parentChain()
	if iptables("-C", parent, "-j", Chain) != nil {
		return iptables("-I", parent, "1", "-j", Chain)
	}

	return nil
}

func (l *Linux) RemoveFirewall() error {
	parent := parentChain()
	for iptables("-C", parent, "-j", Chain) == nil {
		if err := iptables("-D", parent, "-j", Chain); err != nil {
			break
		}
	}
	_ = iptables("-F", Chain)
	_ = iptables("-X", Chain)

	return nil
}

// ---- containers ---------------------------------------------------------------------------------------------------------

func (l *Linux) Attached() ([]Attached, error) {
	cli, err := l.docker()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	existing, err := cli.NetworkInspect(ctx, networkName, network.InspectOptions{})
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]Attached, 0, len(existing.Containers))
	for _, c := range existing.Containers {
		address := c.IPv4Address
		if i := strings.Index(address, "/"); i >= 0 {
			address = address[:i]
		}
		out = append(out, Attached{Container: c.Name, Address: address})
	}

	return out, nil
}

func (l *Linux) Exists(container string) (bool, error) {
	cli, err := l.docker()
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := cli.ContainerInspect(ctx, container); err != nil {
		if cerrdefs.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (l *Linux) Attach(container, address string) error {
	cli, err := l.docker()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return cli.NetworkConnect(ctx, networkName, container, &network.EndpointSettings{
		IPAMConfig: &network.EndpointIPAMConfig{IPv4Address: address},
	})
}

func (l *Linux) Detach(container string) error {
	cli, err := l.docker()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return cli.NetworkDisconnect(ctx, networkName, container, true)
}
