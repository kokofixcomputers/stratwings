package docker

import "testing"

func TestCpuBurstMicroseconds(t *testing.T) {
	tests := []struct {
		name     string
		quota    int64
		percent  int64
		expected int64
	}{
		{name: "full quota", quota: 200_000, percent: 100, expected: 200_000},
		{name: "half quota", quota: 200_000, percent: 50, expected: 100_000},
		{name: "zero percent", quota: 200_000, percent: 0, expected: 0},
		{name: "percent above kernel cap", quota: 200_000, percent: 150, expected: 200_000},
		{name: "negative percent", quota: 200_000, percent: -50, expected: 0},
		{name: "no quota", quota: 0, percent: 100, expected: 0},
		{name: "negative quota", quota: -1, percent: 100, expected: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if v := cpuBurstMicroseconds(tt.quota, tt.percent); v != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, v)
			}
		})
	}
}

func TestResolveCgroupCpuFile(t *testing.T) {
	tests := []struct {
		name       string
		procCgroup string
		v2         bool
		expected   string
		wantErr    bool
	}{
		{
			name:       "v2 systemd scope",
			procCgroup: "0::/system.slice/docker-abc123.scope\n",
			v2:         true,
			expected:   "/sys/fs/cgroup/system.slice/docker-abc123.scope/cpu.max.burst",
		},
		{
			name:       "v2 rootless",
			procCgroup: "0::/user.slice/user-1000.slice/user@1000.service/user.slice/docker-abc123.scope\n",
			v2:         true,
			expected:   "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/user.slice/docker-abc123.scope/cpu.max.burst",
		},
		{
			name:       "v1 combined cpu controller",
			procCgroup: "12:pids:/docker/abc123\n4:cpu,cpuacct:/docker/abc123\n1:name=systemd:/docker/abc123\n",
			v2:         false,
			expected:   "/sys/fs/cgroup/cpu/docker/abc123/cpu.cfs_burst_us",
		},
		{
			name:       "v1 bare cpu controller",
			procCgroup: "4:cpu:/docker/abc123\n",
			v2:         false,
			expected:   "/sys/fs/cgroup/cpu/docker/abc123/cpu.cfs_burst_us",
		},
		{
			name:       "v1 cpuset and cpuacct do not match",
			procCgroup: "5:cpuset:/docker/abc123\n4:cpuacct:/docker/abc123\n",
			v2:         false,
			wantErr:    true,
		},
		{
			name:       "v1 host ignores the unified hierarchy line",
			procCgroup: "0::/docker/abc123\n",
			v2:         false,
			wantErr:    true,
		},
		{
			name:       "namespaced relative path",
			procCgroup: "0::/../../system.slice\n",
			v2:         true,
			wantErr:    true,
		},
		{
			name:       "empty content",
			procCgroup: "",
			v2:         true,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := resolveCgroupCpuFile(tt.procCgroup, tt.v2)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected an error, got %q", f)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if f != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, f)
			}
		})
	}
}

func TestParseCpuBurstStats(t *testing.T) {
	v2 := "usage_usec 100\nuser_usec 60\nsystem_usec 40\nnr_periods 50\nnr_throttled 4\nthrottled_usec 800\nnr_bursts 3\nburst_usec 1500\n"
	got := parseCpuBurstStats(v2, true)
	if got == nil || got.Periods != 3 || got.Microseconds != 1500 {
		t.Fatalf("v2: unexpected %+v", got)
	}

	// cgroup v1 counts the burst time in nanoseconds.
	v1 := "nr_periods 50\nnr_throttled 4\nthrottled_time 800000\nnr_bursts 2\nburst_time 2500000\n"
	got = parseCpuBurstStats(v1, false)
	if got == nil || got.Periods != 2 || got.Microseconds != 2500 {
		t.Fatalf("v1: unexpected %+v", got)
	}

	// A kernel older than 5.14 has no burst counters at all.
	if parseCpuBurstStats("nr_periods 5\nnr_throttled 0\nthrottled_usec 0\n", true) != nil {
		t.Fatal("expected nil without burst counters")
	}
	if parseCpuBurstStats("", true) != nil || parseCpuBurstStats("nr_bursts x\nburst_usec 1\n", true) != nil {
		t.Fatal("expected nil for an unreadable file")
	}
}

func TestResolveCgroupCpuStatFile(t *testing.T) {
	f, err := resolveCgroupCpuStatFile("0::/system.slice/docker-abc123.scope\n", true)
	if err != nil || f != "/sys/fs/cgroup/system.slice/docker-abc123.scope/cpu.stat" {
		t.Fatalf("v2: %q %v", f, err)
	}
	f, err = resolveCgroupCpuStatFile("4:cpu,cpuacct:/docker/abc123\n1:name=systemd:/docker/abc123\n", false)
	if err != nil || f != "/sys/fs/cgroup/cpu/docker/abc123/cpu.stat" {
		t.Fatalf("v1: %q %v", f, err)
	}
	if _, err := resolveCgroupCpuStatFile("0::/../../etc\n", true); err == nil {
		t.Fatal("a path that climbs out of the cgroup must be refused")
	}
}
