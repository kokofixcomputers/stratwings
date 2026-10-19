package environment

// Stats defines the current resource usage for a given server instance.
type Stats struct {
	// The total amount of memory, in bytes, that this server instance is consuming. This is
	// calculated slightly differently than just using the raw Memory field that the stats
	// return from the container, so please check the code setting this value for how that
	// is calculated.
	Memory uint64 `json:"memory_bytes"`

	// The total amount of memory this container or resource can use. Inside Docker this is
	// going to be higher than you'd expect because we're automatically allocating overhead
	// abilities for the container, so it's not going to be a perfect match.
	MemoryLimit uint64 `json:"memory_limit_bytes"`

	// The absolute CPU usage is the amount of CPU used in relation to the entire system and
	// does not take into account any limits on the server process itself.
	CpuAbsolute float64 `json:"cpu_absolute"`

	// How much the container has spent of the CFS burst, from the cpu.stat of its cgroup. Nil when
	// it cannot be read (an older kernel, or a cgroup Wings cannot see). The numbers only grow, a
	// reader looks at the difference between two samples to know whether the server is bursting now.
	CpuBurst *CpuBurstStats `json:"cpu_burst,omitempty"`

	// Current network transmit in & out for a container.
	Network NetworkStats `json:"network"`

	// The current uptime of the container, in milliseconds.
	Uptime int64 `json:"uptime"`
}

type NetworkStats struct {
	RxBytes uint64 `json:"rx_bytes"`
	TxBytes uint64 `json:"tx_bytes"`
}

// CpuBurstStats are the cumulative counters of the kernel for CFS bursting since the container
// was created.
type CpuBurstStats struct {
	// Periods is the number of scheduling periods in which the container used burst (nr_bursts).
	Periods uint64 `json:"periods"`

	// Microseconds is the time spent on burst in total (burst_usec).
	Microseconds uint64 `json:"microseconds"`
}
