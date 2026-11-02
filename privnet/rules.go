package privnet

import (
	"net"
	"sort"
	"strings"
)

// Chain is the iptables chain with the rules of the private network. It is put at the top of DOCKER-USER, which Docker keeps for rules of
// the admin and looks at before its own.
const Chain = "STRAT-PRIVATE"

// Within says whether an address is in a range (10.99.3.0/24).
func Within(cidr, address string) bool {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(address)

	return ip != nil && network.Contains(ip)
}

// Rules are the rules of the chain, as arguments of `iptables -A STRAT-PRIVATE`: what belongs to a connection that is open is let through,
// a server is reached by the other servers of its owner and nobody else, and the rest of the private range is closed. The nodes see the
// servers of all nodes and make the rules for their own, which is where traffic to them is judged.
func Rules(config Config) []string {
	rules := []string{"-m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT"}

	groups := map[int][]string{}
	for _, member := range config.Servers {
		groups[member.Group] = append(groups[member.Group], member.Address)
	}
	local := make([]Member, 0)
	for _, member := range config.Servers {
		if Within(config.Subnet, member.Address) {
			local = append(local, member)
		}
	}
	sort.Slice(local, func(i, j int) bool { return local[i].Address < local[j].Address })

	for _, member := range local {
		var sources []string
		for _, address := range groups[member.Group] {
			if address != member.Address {
				sources = append(sources, address)
			}
		}
		if len(sources) == 0 {
			continue
		}
		sort.Strings(sources)
		rules = append(rules, "-d "+member.Address+" -s "+strings.Join(sources, ",")+" -j ACCEPT")
	}

	return append(rules, "-d "+config.Range+" -j DROP")
}
