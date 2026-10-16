package cmd

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/apex/log"
	"github.com/spf13/cobra"

	"github.com/pterodactyl/wings/internal/selfupdate"
)

const serviceFile = "/etc/systemd/system/wings.service"

const serviceTemplate = `[Unit]
Description=Stratosphere Wings Daemon
After=docker.service
Requires=docker.service
PartOf=docker.service

[Service]
User=root
WorkingDirectory=/etc/pterodactyl
LimitNOFILE=4096
PIDFile=/var/run/wings/daemon.pid
ExecStart=%s
Restart=on-failure
StartLimitInterval=180
StartLimitBurst=30
RestartSec=5s

[Install]
WantedBy=multi-user.target
`

// installCommand sets a node up in one go: it fetches the configuration with the token of the node, writes the service and starts it.
var installCommand = &cobra.Command{
	Use:   "install <panel-url> <token> [node-id]",
	Short: "Configure this node from the panel, install the systemd service and start it",
	Args:  cobra.RangeArgs(2, 3),
	Run: func(cmd *cobra.Command, args []string) {
		if os.Geteuid() != 0 {
			fmt.Println("Run this as root (sudo).")
			os.Exit(1)
		}
		exe, err := os.Executable()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if err := os.MkdirAll("/etc/pterodactyl", 0o755); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		configureArgs.PanelURL = args[0]
		configureArgs.Token = args[1]
		if len(args) == 3 {
			configureArgs.Node = args[2]
		}
		configureArgs.Override = true
		configureCmdRun(cmd, nil)

		if _, err := os.Stat(serviceFile); err != nil {
			if err := os.WriteFile(serviceFile, []byte(fmt.Sprintf(serviceTemplate, exe)), 0o644); err != nil {
				fmt.Println("Could not write the service:", err)
				os.Exit(1)
			}
		}
		for _, c := range [][]string{{"daemon-reload"}, {"enable", "--now", "wings"}} {
			if out, err := exec.Command("systemctl", c...).CombinedOutput(); err != nil {
				fmt.Printf("systemctl %v failed: %s\n", c, out)
				os.Exit(1)
			}
		}
		fmt.Println("Wings is installed and running. Check it with: systemctl status wings")
	},
}

// bootCheckUpdate puts the previous binary back when a fresh update keeps failing to start.
func bootCheckUpdate() {
	exe, err := os.Executable()
	if err != nil {
		return
	}
	if selfupdate.Boot(exe) {
		log.Error("the updated wings did not stay up, going back to the previous version")
		_ = selfupdate.Restart(exe)
	}
}
