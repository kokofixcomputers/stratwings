// Package jobs runs long work in the background, for a server, and keeps what is known about it: the page of the Panel can be closed or
// reloaded and the work goes on, and the result is still there afterwards. Jobs are kept on disk, work that was running when Wings
// stopped is shown as interrupted.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Status is where a job is.
type Status string

const (
	Scanning    Status = "scanning"
	Running     Status = "running"
	Finished    Status = "finished"
	Failed      Status = "failed"
	Cancelled   Status = "cancelled"
	Interrupted Status = "interrupted"
)

// Active tells whether the job is still going.
func (s Status) Active() bool { return s == Scanning || s == Running }

const (
	maxFailures = 200
	maxWarnings = 20
	// How many finished jobs of a server are kept, and for how long.
	keepJobs = 20
	keepFor  = 14 * 24 * time.Hour
)

// Failure is a file that could not be done.
type Failure struct {
	Path  string `json:"path"`
	Error string `json:"error"`
}

// Job is everything the Panel shows of a piece of work. It never holds a secret.
type Job struct {
	ID     string         `json:"id"`
	Server string         `json:"server"`
	Type   string         `json:"type"`
	Title  string         `json:"title"`
	Status Status         `json:"status"`
	Params map[string]any `json:"params,omitempty"`

	// While looking at what there is to do.
	Scanned  int    `json:"scanned"`
	Folders  int    `json:"folders"`
	ScanPath string `json:"scan_path"`

	Files      int    `json:"files"`
	Dirs       int    `json:"dirs"`
	Done       int    `json:"done"`
	Skipped    int    `json:"skipped"`
	Failed     int    `json:"failed"`
	Bytes      int64  `json:"bytes"`
	BytesTotal int64  `json:"bytes_total"`
	Current    string `json:"current"`

	Failures []Failure `json:"failures"`
	Warnings []string  `json:"warnings"`
	Error    string    `json:"error,omitempty"`

	// What the job made, such as the name of the archive.
	Result map[string]any `json:"result,omitempty"`

	CreatedAt  time.Time  `json:"created_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// Quick jobs that went well are forgotten after a while, so they do not push the interesting ones out of the list.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// Options say how a job is treated.
type Options struct {
	// Group: only one active job of a group goes at a time for a server (empty: no limit).
	Group string
	// SuccessTTL: how long a job that finished without a failure is kept (0: as long as the others).
	SuccessTTL time.Duration
}

// Update changes a job while it runs.
type Update func(func(*Job))

// Work is what a job does. It has to stop when ctx ends. A nil error is a finished job, an error a failed one (a cancelled job is
// recognised by its context).
type Work func(ctx context.Context, update Update) error

// ErrBusy is returned when the server already has a job of that group going.
var ErrBusy = errors.New("a job of this kind is already running for this server")

type entry struct {
	job    Job
	opts   Options
	cancel context.CancelFunc
	dirty  bool
}

// Manager is safe for concurrent use.
type Manager struct {
	dir string

	mu      sync.Mutex
	entries map[string]*entry
	stop    chan struct{}
	running sync.WaitGroup
	closed  sync.Once
}

// NewManager reads the jobs in dir. Jobs that were going when the node stopped can not be going now: they become interrupted.
func NewManager(dir string) (*Manager, error) {
	m := &Manager{dir: dir, entries: map[string]*entry{}, stop: make(chan struct{})}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	servers, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	for _, s := range servers {
		if !s.IsDir() {
			continue
		}
		files, _ := os.ReadDir(filepath.Join(dir, s.Name()))
		for _, f := range files {
			raw, err := os.ReadFile(filepath.Join(dir, s.Name(), f.Name()))
			if err != nil {
				continue
			}
			var j Job
			if json.Unmarshal(raw, &j) != nil || j.ID == "" || j.Server != s.Name() {
				continue
			}
			if j.Status.Active() {
				j.Status = Interrupted
				j.Error = "The node was restarted while this was running. What was done is kept; run it again to continue (existing files can be skipped)."
				j.FinishedAt = &now
				m.entries[j.ID] = &entry{job: j, dirty: true}
				m.persist(m.entries[j.ID])
				continue
			}
			m.entries[j.ID] = &entry{job: j}
		}
	}
	m.prune()

	go m.flusher()

	return m, nil
}

// Close stops the writer that saves progress.
func (m *Manager) Close() { m.closed.Do(func() { close(m.stop) }) }

// Shutdown cancels the jobs that are going, waits until they are over (they are saved as cancelled) and stops the writer.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	for _, e := range m.entries {
		if e.job.Status.Active() && e.cancel != nil {
			e.cancel()
		}
	}
	m.mu.Unlock()
	m.running.Wait()
	m.Close()
}

func (m *Manager) path(j Job) string { return filepath.Join(m.dir, j.Server, j.ID+".json") }

func (m *Manager) persist(e *entry) {
	if err := os.MkdirAll(filepath.Join(m.dir, e.job.Server), 0o700); err != nil {
		return
	}
	raw, err := json.Marshal(e.job)
	if err != nil {
		return
	}
	tmp := m.path(e.job) + ".tmp"
	if os.WriteFile(tmp, raw, 0o600) == nil {
		_ = os.Rename(tmp, m.path(e.job))
	}
	e.dirty = false
}

// flusher saves the progress of running jobs, not on every file.
func (m *Manager) flusher() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.mu.Lock()
			for _, e := range m.entries {
				if e.dirty {
					m.persist(e)
				}
			}
			m.prune()
			m.mu.Unlock()
		case <-m.stop:
			return
		}
	}
}

// Start begins a job in the background.
func (m *Manager) Start(server, typ, title string, params map[string]any, opts Options, work Work) (Job, error) {
	m.mu.Lock()
	for _, e := range m.entries {
		if opts.Group != "" && e.job.Server == server && e.opts.Group == opts.Group && e.job.Status.Active() {
			m.mu.Unlock()

			return Job{}, ErrBusy
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &entry{
		cancel: cancel,
		opts:   opts,
		job: Job{
			ID: uuid.NewString(), Server: server, Type: typ, Title: title, Status: Scanning, Params: params,
			Failures: []Failure{}, Warnings: []string{}, CreatedAt: time.Now(),
		},
	}
	m.entries[e.job.ID] = e
	m.persist(e)
	snapshot := e.job
	m.mu.Unlock()

	m.running.Add(1)
	go func() {
		defer m.running.Done()
		err := m.run(ctx, e, work)
		// Work that ended without an error is finished, also when a cancel came in at the last moment.
		m.finish(e, err, err != nil && ctx.Err() != nil)
	}()

	return snapshot, nil
}

func (m *Manager) run(ctx context.Context, e *entry, work Work) (err error) {
	// A job that panics must not take the node down with it.
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("the job crashed")
		}
	}()

	return work(ctx, func(f func(*Job)) {
		m.mu.Lock()
		defer m.mu.Unlock()
		f(&e.job)
		e.dirty = true
		if len(e.job.Failures) > maxFailures {
			e.job.Failures = e.job.Failures[:maxFailures]
		}
		if len(e.job.Warnings) > maxWarnings {
			e.job.Warnings = e.job.Warnings[:maxWarnings]
		}
	})
}

func (m *Manager) finish(e *entry, err error, cancelled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	e.job.FinishedAt = &now
	e.job.Current = ""
	switch {
	case cancelled:
		e.job.Status = Cancelled
	case err != nil:
		e.job.Status = Failed
		e.job.Error = err.Error()
	default:
		e.job.Status = Finished
		if e.opts.SuccessTTL > 0 && e.job.Failed == 0 {
			at := now.Add(e.opts.SuccessTTL)
			e.job.ExpiresAt = &at
		}
	}
	e.cancel()
	m.persist(e)
	m.prune()
}

// Get returns a copy of a job of the server.
func (m *Manager) Get(server, id string) (Job, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[id]
	if !ok || e.job.Server != server {
		return Job{}, false
	}

	return e.job, true
}

// List gives the jobs of a server, the newest first.
func (m *Manager) List(server string) []Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Job{}
	for _, e := range m.entries {
		if e.job.Server == server {
			out = append(out, e.job)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })

	return out
}

// Cancel stops a job that is going. It returns false for a job that is not there or already over.
func (m *Manager) Cancel(server, id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[id]
	if !ok || e.job.Server != server || !e.job.Status.Active() || e.cancel == nil {
		return false
	}
	e.cancel()

	return true
}

// Remove forgets a job that is over.
func (m *Manager) Remove(server, id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[id]
	if !ok || e.job.Server != server || e.job.Status.Active() {
		return false
	}
	delete(m.entries, id)
	_ = os.Remove(m.path(e.job))

	return true
}

// prune forgets old jobs, the caller holds the lock.
func (m *Manager) prune() {
	byServer := map[string][]*entry{}
	for _, e := range m.entries {
		if e.job.Status.Active() {
			continue
		}
		if e.job.ExpiresAt != nil && time.Now().After(*e.job.ExpiresAt) {
			delete(m.entries, e.job.ID)
			_ = os.Remove(m.path(e.job))

			continue
		}
		byServer[e.job.Server] = append(byServer[e.job.Server], e)
	}
	for _, list := range byServer {
		sort.Slice(list, func(i, j int) bool { return list[i].job.CreatedAt.After(list[j].job.CreatedAt) })
		for i, e := range list {
			if i >= keepJobs || time.Since(e.job.CreatedAt) > keepFor {
				delete(m.entries, e.job.ID)
				_ = os.Remove(m.path(e.job))
			}
		}
	}
}

// ---- slots ----------------------------------------------------------------------------------------------------------------

// Slots is a limit on what a server does at the same time across jobs (a node lets a server download three files at once, however
// many jobs it has). Take one with Acquire and give it back with the function it returns.
type Slots struct {
	mu   sync.Mutex
	sems map[string]chan struct{}
}

// NewSlots makes an empty set of limits.
func NewSlots() *Slots { return &Slots{sems: map[string]chan struct{}{}} }

// Acquire waits for one of n slots of the key. It returns the release function, or an error when ctx ends first.
func (s *Slots) Acquire(ctx context.Context, key string, n int) (func(), error) {
	s.mu.Lock()
	sem, ok := s.sems[key]
	if !ok {
		sem = make(chan struct{}, n)
		s.sems[key] = sem
	}
	s.mu.Unlock()

	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}
}

// ---- the manager of this node -----------------------------------------------------------------------------------------------

var (
	sharedMu sync.RWMutex
	shared   *Manager
)

// SetShared sets the manager the routes use.
func SetShared(m *Manager) {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	shared = m
}

// Shared is the manager of this node, nil until set.
func Shared() *Manager {
	sharedMu.RLock()
	defer sharedMu.RUnlock()

	return shared
}

// ShutdownShared stops the jobs of this node, waiting for them for at most the given time (a read that does not come back must not keep
// the node from stopping).
func ShutdownShared(wait time.Duration) {
	m := Shared()
	if m == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		m.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(wait):
	}
}
