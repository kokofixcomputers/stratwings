package config

import "testing"

func TestDockerRegistryCredentialsForImage(t *testing.T) {
	cfg := DockerConfiguration{
		Registries: map[string]RegistryConfiguration{
			"registry.example.com": {
				Username: "registry",
				Password: "secret",
			},
			"registry.example.com/team": {
				Username: "team",
				Password: "secret",
			},
			"registry.example.com:5000": {
				Username: "port",
				Password: "secret",
			},
			"https://index.docker.io/v1/": {
				Username: "docker",
				Password: "secret",
			},
		},
	}

	tests := []struct {
		name     string
		image    string
		username string
	}{
		{
			name:     "registry domain",
			image:    "registry.example.com/project/image:latest",
			username: "registry",
		},
		{
			name:     "registry with port",
			image:    "registry.example.com:5000/project/image:latest",
			username: "port",
		},
		{
			name:     "registry path",
			image:    "registry.example.com/team/image:latest",
			username: "team",
		},
		{
			name:  "registry prefix is not domain",
			image: "registry.example.com.evil/project/image:latest",
		},
		{
			name:     "registry path prefix falls back to domain",
			image:    "registry.example.com/team-evil/image:latest",
			username: "registry",
		},
		{
			name:     "legacy docker hub registry",
			image:    "docker.io/library/busybox:latest",
			username: "docker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, registry := cfg.RegistryCredentialsForImage(tt.image)
			if tt.username == "" {
				if registry != nil {
					t.Fatalf("expected no registry credentials, got username %q", registry.Username)
				}

				return
			}

			if registry == nil {
				t.Fatalf("expected registry credentials for %q", tt.image)
			}

			if registry.Username != tt.username {
				t.Fatalf("expected username %q, got %q", tt.username, registry.Username)
			}
		})
	}
}

func TestCpuPeriodMicroseconds(t *testing.T) {
	tests := []struct {
		name     string
		period   int64
		expected int64
	}{
		{name: "default period", period: 100_000, expected: 100_000},
		{name: "shorter period", period: 20_000, expected: 20_000},
		{name: "below kernel minimum", period: 500, expected: 1_000},
		{name: "above kernel maximum", period: 5_000_000, expected: 1_000_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DockerConfiguration{CpuPeriod: tt.period}
			if v := cfg.CpuPeriodMicroseconds(); v != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, v)
			}
		})
	}
}

func TestDockerRegistryPathCredentialsDoNotMatchSiblingPath(t *testing.T) {
	cfg := DockerConfiguration{
		Registries: map[string]RegistryConfiguration{
			"registry.example.com/team": {
				Username: "team",
				Password: "secret",
			},
		},
	}

	_, registry := cfg.RegistryCredentialsForImage("registry.example.com/team-evil/image:latest")
	if registry != nil {
		t.Fatalf("expected no registry credentials, got username %q", registry.Username)
	}
}

func TestPanelConfigurationCarriesCpuBurst(t *testing.T) {
	c := &Configuration{}
	c.Docker.CpuBurst.Enabled = true
	c.Docker.CpuBurst.Percent = 100

	// What the panel sends is applied, and a missing value keeps the current one.
	p := c.PanelConfiguration()
	if !p.Docker.CpuBurst.Enabled || p.Docker.CpuBurst.Percent != 100 {
		t.Fatalf("the current burst setting is part of the panel's view: %+v", p.Docker.CpuBurst)
	}
	p.Docker.CpuBurst.Enabled = false
	p.Docker.CpuBurst.Percent = 40
	c.ApplyPanelConfiguration(p)
	if c.Docker.CpuBurst.Enabled || c.Docker.CpuBurst.Percent != 40 {
		t.Fatalf("burst was not applied: %+v", c.Docker.CpuBurst)
	}

	// Nothing the kernel would refuse gets through.
	p.Docker.CpuBurst.Percent = 900
	c.ApplyPanelConfiguration(p)
	if c.Docker.CpuBurst.Percent != 100 {
		t.Fatalf("percent above 100 must be capped, got %d", c.Docker.CpuBurst.Percent)
	}
	p.Docker.CpuBurst.Percent = -5
	c.ApplyPanelConfiguration(p)
	if c.Docker.CpuBurst.Percent != 0 {
		t.Fatalf("a negative percent must become 0, got %d", c.Docker.CpuBurst.Percent)
	}
}
