package docker

import (
	"context"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/docker/docker/client"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/environment"
)

// cgroupV2 reports whether the host uses the unified cgroup v2 hierarchy.
var cgroupV2 = sync.OnceValue(func() bool {
	_, err := os.Stat("/sys/fs/cgroup/cgroup.controllers")
	return err == nil
})

var burstWarning sync.Once

// cpuBurstMicroseconds returns the burst allowance in microseconds for the given
// CFS quota and configured percentage. The kernel rejects a burst larger than the
// quota, so the value is clamped to it.
func cpuBurstMicroseconds(quota int64, percent int64) int64 {
	if quota <= 0 || percent <= 0 {
		return 0
	}
	if percent > 100 {
		percent = 100
	}
	return quota * percent / 100
}

// resolveCgroupCpuFile parses the contents of a /proc/<pid>/cgroup file and
// returns the absolute path of the CFS burst file for that process's cgroup.
func resolveCgroupCpuFile(procCgroup string, v2 bool) (string, error) {
	if v2 {
		return resolveCgroupFile(procCgroup, true, "cpu.max.burst", "")
	}
	return resolveCgroupFile(procCgroup, false, "", "cpu.cfs_burst_us")
}

// resolveCgroupCpuStatFile is the cpu.stat file of the same cgroup, which holds the burst counters.
func resolveCgroupCpuStatFile(procCgroup string, v2 bool) (string, error) {
	return resolveCgroupFile(procCgroup, v2, "cpu.stat", "cpu.stat")
}

// resolveCgroupFile finds a file of the cpu cgroup in the contents of /proc/<pid>/cgroup, named
// differently on cgroup v2 and v1.
func resolveCgroupFile(procCgroup string, v2 bool, v2name string, v1name string) (string, error) {
	for _, line := range strings.Split(procCgroup, "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 || !strings.HasPrefix(parts[2], "/") || strings.Contains(parts[2], "..") {
			continue
		}
		if v2 {
			if parts[0] == "0" && parts[1] == "" {
				return path.Join("/sys/fs/cgroup", parts[2], v2name), nil
			}
			continue
		}
		for _, controller := range strings.Split(parts[1], ",") {
			if controller == "cpu" {
				return path.Join("/sys/fs/cgroup/cpu", parts[2], v1name), nil
			}
		}
	}
	return "", errors.New("environment/docker: no cpu controller found in cgroup file")
}

// writeCpuBurst writes a burst value in microseconds into the cpu cgroup of the
// given process. This is expected to fail on kernels older than 5.14 or when the
// cgroup hierarchy is not writable by Wings, so failures are only logged.
func writeCpuBurst(l *log.Entry, pid int, burst int64) {
	if pid <= 0 {
		return
	}
	if err := writeBurstFile(pid, burst); err != nil {
		logBurstFailure(l.WithField("error", err), burst)
		return
	}
	l.WithField("burst_us", burst).Debug("updated container cpu burst")
}

func writeBurstFile(pid int, burst int64) error {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cgroup")
	if err != nil {
		return err
	}
	f, err := resolveCgroupCpuFile(string(b), cgroupV2())
	if err != nil {
		return err
	}
	return os.WriteFile(f, []byte(strconv.FormatInt(burst, 10)), 0o644) //nolint:gosec // an existing cgroupfs file, the mode is never applied
}

// logBurstFailure warns the first time a burst cannot be applied and stays at
// debug otherwise. Failed clears are always quiet since a host that never
// accepted a burst has nothing to clear.
func logBurstFailure(l *log.Entry, burst int64) {
	if burst > 0 {
		first := false
		burstWarning.Do(func() { first = true })
		if first {
			l.Warn("failed to set cpu burst, this requires Linux 5.14 or newer and a writable cgroup hierarchy")
			return
		}
	}
	l.Debug("failed to set cpu burst")
}

// SetCpuBurst applies the configured CFS burst to a running container based on
// the CFS quota in microseconds it was created with. This is a no-op when
// bursting is disabled or the container has no CPU limit.
func SetCpuBurst(ctx context.Context, cli *client.Client, containerID string, quota int64) {
	cfg := config.Get().Docker.CpuBurst
	if quota <= 0 {
		return
	}
	c, err := cli.ContainerInspect(ctx, containerID)
	if err != nil || c.State == nil {
		return
	}
	// Switched off: a burst that was set while it was on must not stay in the cgroup of a container that lives on.
	if !cfg.Enabled {
		writeCpuBurst(log.WithField("container_id", containerID), c.State.Pid, 0)
		return
	}
	writeCpuBurst(log.WithField("container_id", containerID), c.State.Pid, cpuBurstMicroseconds(quota, cfg.Percent))
}

// applyCpuBurst applies the configured CFS burst to the environment's container
// using its current CPU limit.
func (e *Environment) applyCpuBurst(ctx context.Context) {
	quota := e.Configuration.Limits().CpuLimit * config.Get().Docker.CpuPeriodMicroseconds() / 100
	SetCpuBurst(ctx, e.client, e.Id, quota)
}

// clearCpuBurst zeroes the CFS burst for the given container process. This must
// happen before a quota change is applied since the kernel rejects a quota lower
// than the current burst. It runs even when bursting is disabled so a value set
// before the feature was turned off cannot block future quota changes.
func (e *Environment) clearCpuBurst(pid int) {
	writeCpuBurst(e.log(), pid, 0)
}

// parseCpuBurstStats reads the burst counters out of the contents of a cpu.stat file: nr_bursts and
// burst_usec on cgroup v2, nr_bursts and burst_time (nanoseconds) on v1. Nil when they are not there,
// which is how a kernel older than 5.14 looks.
func parseCpuBurstStats(content string, v2 bool) *environment.CpuBurstStats {
	var periods, amount uint64
	havePeriods, haveAmount := false, false
	key := "burst_usec"
	if !v2 {
		key = "burst_time"
	}
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "nr_bursts":
			periods, havePeriods = v, true
		case key:
			amount, haveAmount = v, true
		}
	}
	if !havePeriods || !haveAmount {
		return nil
	}
	if !v2 {
		amount /= 1000
	}
	return &environment.CpuBurstStats{Periods: periods, Microseconds: amount}
}

// cpuStatFile finds the cpu.stat file of the container's cgroup. Empty when it cannot be found.
func cpuStatFile(pid int) string {
	if pid <= 0 {
		return ""
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cgroup")
	if err != nil {
		return ""
	}
	f, err := resolveCgroupCpuStatFile(string(b), cgroupV2())
	if err != nil {
		return ""
	}
	return f
}

// readCpuBurstStats reads the burst counters from a cpu.stat file, nil when that fails.
func readCpuBurstStats(file string) *environment.CpuBurstStats {
	if file == "" {
		return nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	return parseCpuBurstStats(string(b), cgroupV2())
}
