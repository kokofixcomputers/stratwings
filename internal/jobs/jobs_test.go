package jobs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pterodactyl/wings/internal/importer"
)

func manager(t *testing.T) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Shutdown)

	return m, dir
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestAJobRunsInTheBackgroundAndIsKept(t *testing.T) {
	m, dir := manager(t)
	release := make(chan struct{})

	job, err := m.Start("srv1", "import", "Import", map[string]any{"host": "h"}, Options{}, func(ctx context.Context, update Update) error {
		update(func(j *Job) { j.Status = Running; j.Files = 10; j.Done = 4 })
		<-release

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != Scanning || job.ID == "" {
		t.Fatalf("%+v", job)
	}

	waitFor(t, "progress", func() bool { j, _ := m.Get("srv1", job.ID); return j.Done == 4 })
	j, _ := m.Get("srv1", job.ID)
	if j.Status != Running || j.Files != 10 {
		t.Errorf("%+v", j)
	}

	close(release)
	waitFor(t, "finish", func() bool { j, _ := m.Get("srv1", job.ID); return j.Status == Finished })
	j, _ = m.Get("srv1", job.ID)
	if j.FinishedAt == nil {
		t.Error("no finish time")
	}
	if _, err := os.Stat(filepath.Join(dir, "srv1", job.ID+".json")); err != nil {
		t.Errorf("not saved: %v", err)
	}
}

func TestOnlyOneJobOfATypeGoesAtATimeForAServer(t *testing.T) {
	m, _ := manager(t)
	release := make(chan struct{})
	work := func(ctx context.Context, _ Update) error { <-release; return nil }

	if _, err := m.Start("srv1", "import", "a", nil, Options{Group: "import"}, work); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start("srv1", "import", "b", nil, Options{Group: "import"}, work); !errors.Is(err, ErrBusy) {
		t.Errorf("a second import was accepted: %v", err)
	}
	// Another server, or another kind, is not in the way.
	if _, err := m.Start("srv2", "import", "c", nil, Options{Group: "import"}, work); err != nil {
		t.Errorf("another server: %v", err)
	}
	if _, err := m.Start("srv1", "other", "d", nil, Options{Group: "other"}, work); err != nil {
		t.Errorf("another kind: %v", err)
	}
	close(release)
}

func TestAFailureAndACrashAreKeptAsFailed(t *testing.T) {
	m, _ := manager(t)

	a, _ := m.Start("srv", "x", "fails", nil, Options{}, func(context.Context, Update) error { return errors.New("no route to host") })
	waitFor(t, "failed", func() bool { j, _ := m.Get("srv", a.ID); return j.Status == Failed })
	if j, _ := m.Get("srv", a.ID); j.Error != "no route to host" {
		t.Errorf("error = %q", j.Error)
	}

	b, _ := m.Start("srv", "y", "crashes", nil, Options{}, func(context.Context, Update) error { panic("boom") })
	waitFor(t, "crash", func() bool { j, _ := m.Get("srv", b.ID); return j.Status == Failed })
	if j, _ := m.Get("srv", b.ID); !strings.Contains(j.Error, "crashed") {
		t.Errorf("error = %q", j.Error)
	}
}

func TestCancellingStopsTheWork(t *testing.T) {
	m, _ := manager(t)
	started := make(chan struct{})

	job, _ := m.Start("srv", "import", "x", nil, Options{}, func(ctx context.Context, _ Update) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})
	<-started
	if !m.Cancel("srv", job.ID) {
		t.Fatal("could not cancel")
	}
	waitFor(t, "cancelled", func() bool { j, _ := m.Get("srv", job.ID); return j.Status == Cancelled })
	if m.Cancel("srv", job.ID) {
		t.Error("a job that is over was cancelled")
	}
	if m.Cancel("other-server", job.ID) {
		t.Error("cancelled the job of another server")
	}
}

func TestJobsOfOtherServersAreNotVisible(t *testing.T) {
	m, _ := manager(t)
	job, _ := m.Start("srv1", "x", "t", nil, Options{}, func(context.Context, Update) error { return nil })
	waitFor(t, "finish", func() bool { j, _ := m.Get("srv1", job.ID); return j.Status == Finished })

	if _, ok := m.Get("srv2", job.ID); ok {
		t.Error("another server can read the job")
	}
	if len(m.List("srv2")) != 0 || len(m.List("srv1")) != 1 {
		t.Error("wrong list")
	}
	if m.Remove("srv2", job.ID) {
		t.Error("another server removed the job")
	}
	if !m.Remove("srv1", job.ID) {
		t.Error("could not remove a finished job")
	}
	if _, ok := m.Get("srv1", job.ID); ok {
		t.Error("still there")
	}
}

func TestAJobThatIsGoingCanNotBeRemoved(t *testing.T) {
	m, _ := manager(t)
	release := make(chan struct{})
	job, _ := m.Start("srv", "x", "t", nil, Options{}, func(context.Context, Update) error { <-release; return nil })
	if m.Remove("srv", job.ID) {
		t.Error("removed a running job")
	}
	close(release)
}

func TestJobsSurviveARestartAndUnfinishedOnesAreInterrupted(t *testing.T) {
	dir := t.TempDir()
	m1, _ := NewManager(dir)
	release := make(chan struct{})
	done, _ := m1.Start("srv", "a", "done", nil, Options{}, func(context.Context, Update) error { return nil })
	waitFor(t, "done", func() bool { j, _ := m1.Get("srv", done.ID); return j.Status == Finished })
	going, _ := m1.Start("srv", "b", "going", nil, Options{}, func(ctx context.Context, update Update) error {
		update(func(j *Job) { j.Status = Running; j.Done = 7 })
		<-release

		return nil
	})
	waitFor(t, "progress", func() bool { j, _ := m1.Get("srv", going.ID); return j.Done == 7 })
	// Wait for the writer to save the progress, then "restart" without finishing the job.
	time.Sleep(2300 * time.Millisecond)
	m1.Close()

	m2, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	// The old process is gone: it must not get to write anything before the checks below.
	defer func() { close(release); m1.Shutdown() }()

	a, ok := m2.Get("srv", done.ID)
	if !ok || a.Status != Finished {
		t.Errorf("finished job after restart: %+v %v", a, ok)
	}
	b, ok := m2.Get("srv", going.ID)
	if !ok || b.Status != Interrupted || b.Done != 7 || b.FinishedAt == nil || !strings.Contains(b.Error, "restarted") {
		t.Errorf("running job after restart: %+v %v", b, ok)
	}
}

func TestOldJobsAreForgotten(t *testing.T) {
	m, _ := manager(t)
	var ids []string
	for i := 0; i < keepJobs+5; i++ {
		j, _ := m.Start("srv", "t"+string(rune('a'+i%20)), "t", nil, Options{}, func(context.Context, Update) error { return nil })
		ids = append(ids, j.ID)
		waitFor(t, "finish", func() bool { x, _ := m.Get("srv", j.ID); return x.Status == Finished })
		time.Sleep(time.Millisecond)
	}
	if n := len(m.List("srv")); n != keepJobs {
		t.Errorf("%d jobs kept, want %d", n, keepJobs)
	}
	if _, ok := m.Get("srv", ids[0]); ok {
		t.Error("the oldest job is still there")
	}
}

func TestTheListIsNewestFirstAndCapsWhatItKeeps(t *testing.T) {
	m, _ := manager(t)
	job, _ := m.Start("srv", "x", "t", nil, Options{}, func(ctx context.Context, update Update) error {
		for i := 0; i < maxFailures+50; i++ {
			update(func(j *Job) {
				j.Failures = append(j.Failures, Failure{Path: "p", Error: "e"})
				j.Warnings = append(j.Warnings, "w")
			})
		}

		return nil
	})
	waitFor(t, "finish", func() bool { j, _ := m.Get("srv", job.ID); return j.Status == Finished })
	j, _ := m.Get("srv", job.ID)
	if len(j.Failures) != maxFailures || len(j.Warnings) != maxWarnings {
		t.Errorf("%d failures, %d warnings", len(j.Failures), len(j.Warnings))
	}
}

// ---- the import as a job ----------------------------------------------------------------------------------------------------

type tree struct {
	mu    sync.Mutex
	files map[string]string
	block chan struct{}
}

func (tr *tree) List(_ context.Context, dir string) ([]importer.Entry, error) {
	switch dir {
	case "/srv":
		return []importer.Entry{{Name: "a.jar", Size: 3}, {Name: "bad.jar", Size: 3}, {Name: "sub", Dir: true}}, nil
	case "/srv/sub":
		return []importer.Entry{{Name: "b.jar", Size: 4}}, nil
	}

	return nil, errors.New("no such folder")
}

func (tr *tree) Open(ctx context.Context, p string) (io.ReadCloser, error) {
	if tr.block != nil {
		select {
		case <-tr.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if strings.HasSuffix(p, "bad.jar") {
		return nil, errors.New("permission denied")
	}

	return io.NopCloser(strings.NewReader(map[string]string{"/srv/a.jar": "aaa", "/srv/sub/b.jar": "bbbb"}[p])), nil
}

func (tr *tree) Concurrency() int    { return 1 }
func (tr *tree) Fingerprint() string { return "" }
func (tr *tree) Close() error        { return nil }

type sink struct {
	mu    sync.Mutex
	files map[string]string
}

func (s *sink) CreateDirectory(string) error { return nil }
func (s *sink) WriteFrom(p string, r io.Reader) (int64, error) {
	b, _ := io.ReadAll(r)
	s.mu.Lock()
	s.files[p] = string(b)
	s.mu.Unlock()

	return int64(len(b)), nil
}
func (s *sink) Exists(string) bool    { return false }
func (s *sink) Denied(string) bool    { return false }
func (s *sink) OutOfSpace(error) bool { return false }

func TestAnImportJobKeepsTheNumbersTheFailuresAndTheResult(t *testing.T) {
	m, _ := manager(t)
	out := &sink{files: map[string]string{}}

	job, err := m.Start("srv", "import", "Import", nil, Options{}, ImportWork(&tree{}, out, importer.Options{Remote: "/srv", Dest: "/"}))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "finish", func() bool { j, _ := m.Get("srv", job.ID); return !j.Status.Active() })

	j, _ := m.Get("srv", job.ID)
	if j.Status != Finished {
		t.Fatalf("%+v", j)
	}
	if j.Files != 3 || j.Done != 3 || j.Failed != 1 || j.BytesTotal != 10 || j.Dirs != 1 {
		t.Errorf("numbers: %+v", j)
	}
	if len(j.Failures) != 1 || j.Failures[0].Path != "bad.jar" || j.Failures[0].Error != "permission denied" {
		t.Errorf("failures: %+v", j.Failures)
	}
	if out.files["/a.jar"] != "aaa" || out.files["/sub/b.jar"] != "bbbb" {
		t.Errorf("files: %v", out.files)
	}
}

func TestAnImportJobThatCanNotReadTheFolderFails(t *testing.T) {
	m, _ := manager(t)

	job, _ := m.Start("srv", "import", "Import", nil, Options{}, ImportWork(&tree{}, &sink{files: map[string]string{}}, importer.Options{Remote: "/missing", Dest: "/"}))
	waitFor(t, "failed", func() bool { j, _ := m.Get("srv", job.ID); return j.Status == Failed })

	if j, _ := m.Get("srv", job.ID); !strings.Contains(j.Error, "could not read /missing") {
		t.Errorf("error = %q", j.Error)
	}
}

func TestCancellingAnImportJobStopsItWhileItCopies(t *testing.T) {
	m, _ := manager(t)
	tr := &tree{block: make(chan struct{})}

	job, _ := m.Start("srv", "import", "Import", nil, Options{}, ImportWork(tr, &sink{files: map[string]string{}}, importer.Options{Remote: "/srv", Dest: "/"}))
	waitFor(t, "copying", func() bool { j, _ := m.Get("srv", job.ID); return j.Status == Running })
	m.Cancel("srv", job.ID)

	waitFor(t, "cancelled", func() bool { j, _ := m.Get("srv", job.ID); return j.Status == Cancelled })
}

// ---- downloads --------------------------------------------------------------------------------------------------------------

func pullItems(n int) []PullItem {
	var items []PullItem
	for i := 0; i < n; i++ {
		items = append(items, PullItem{URL: "https://cdn.example/m" + string(rune('a'+i)) + ".jar", Directory: "/mods", Filename: "m" + string(rune('a'+i)) + ".jar"})
	}

	return items
}

func TestPullsAreDoneAndCounted(t *testing.T) {
	old := pullRetryWait
	pullRetryWait = 0
	defer func() { pullRetryWait = old }()
	m, _ := manager(t)
	slots := NewSlots()

	job, _ := m.Start("srv", "pull", "Download", nil, Options{}, PullWork("srv", pullItems(5), func(_ context.Context, item PullItem, report func(int64, int64)) error {
		report(100, 100)

		return nil
	}, slots))
	waitFor(t, "finish", func() bool { j, _ := m.Get("srv", job.ID); return !j.Status.Active() })

	j, _ := m.Get("srv", job.ID)
	if j.Status != Finished || j.Files != 5 || j.Done != 5 || j.Failed != 0 || j.Bytes != 500 {
		t.Errorf("%+v", j)
	}
}

func TestAFailedPullIsTriedAgainAndThenReported(t *testing.T) {
	old := pullRetryWait
	pullRetryWait = 0
	defer func() { pullRetryWait = old }()
	m, _ := manager(t)
	var mu sync.Mutex
	tries := map[string]int{}

	job, _ := m.Start("srv", "pull", "Download", nil, Options{}, PullWork("srv", pullItems(3), func(_ context.Context, item PullItem, _ func(int64, int64)) error {
		mu.Lock()
		tries[item.Filename]++
		n := tries[item.Filename]
		mu.Unlock()
		switch item.Filename {
		case "mb.jar":
			return errors.New("got bad response status from endpoint: 404 Not Found")
		case "mc.jar":
			if n < 2 {
				return errors.New("connection reset")
			}
		}

		return nil
	}, NewSlots()))
	waitFor(t, "finish", func() bool { j, _ := m.Get("srv", job.ID); return !j.Status.Active() })

	j, _ := m.Get("srv", job.ID)
	if j.Done != 3 || j.Failed != 1 || len(j.Failures) != 1 || j.Failures[0].Path != "mb.jar" || !strings.Contains(j.Failures[0].Error, "404") {
		t.Errorf("%+v", j)
	}
	if tries["mb.jar"] != 3 || tries["mc.jar"] != 2 || tries["ma.jar"] != 1 {
		t.Errorf("attempts %v", tries)
	}
}

func TestAServerNeverDownloadsMoreThanThreeFilesAtOnceAcrossJobs(t *testing.T) {
	m, _ := manager(t)
	slots := NewSlots()
	var mu sync.Mutex
	active, most := 0, 0
	pull := func(ctx context.Context, _ PullItem, _ func(int64, int64)) error {
		mu.Lock()
		active++
		if active > most {
			most = active
		}
		mu.Unlock()
		time.Sleep(15 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()

		return nil
	}

	a, _ := m.Start("srv", "pull", "A", nil, Options{}, PullWork("srv", pullItems(9), pull, slots))
	b, _ := m.Start("srv", "pull", "B", nil, Options{}, PullWork("srv", pullItems(9), pull, slots))
	waitFor(t, "both", func() bool {
		x, _ := m.Get("srv", a.ID)
		y, _ := m.Get("srv", b.ID)

		return !x.Status.Active() && !y.Status.Active()
	})

	if most > PullConcurrency || most < 2 {
		t.Errorf("at most %d at once", most)
	}
	if j, _ := m.Get("srv", a.ID); j.Done != 9 {
		t.Errorf("%+v", j)
	}
}

func TestCancellingPullsStopsThem(t *testing.T) {
	m, _ := manager(t)
	started := make(chan struct{}, 3)

	job, _ := m.Start("srv", "pull", "Download", nil, Options{}, PullWork("srv", pullItems(8), func(ctx context.Context, _ PullItem, _ func(int64, int64)) error {
		started <- struct{}{}
		<-ctx.Done()

		return ctx.Err()
	}, NewSlots()))
	<-started
	m.Cancel("srv", job.ID)
	waitFor(t, "cancelled", func() bool { j, _ := m.Get("srv", job.ID); return j.Status == Cancelled })

	if j, _ := m.Get("srv", job.ID); j.Done != 0 || j.Failed != 0 {
		t.Errorf("cancelled work was counted: %+v", j)
	}
}

func TestSingleDownloadsKnowTheirSize(t *testing.T) {
	m, _ := manager(t)
	release := make(chan struct{})

	job, _ := m.Start("srv", "pull", "Download", nil, Options{}, PullWork("srv", pullItems(1), func(ctx context.Context, _ PullItem, report func(int64, int64)) error {
		report(250, 1000)
		<-release

		return nil
	}, NewSlots()))
	waitFor(t, "bytes", func() bool { j, _ := m.Get("srv", job.ID); return j.Bytes == 250 })
	if j, _ := m.Get("srv", job.ID); j.BytesTotal != 1000 {
		t.Errorf("%+v", j)
	}
	close(release)
}

func TestQuickSuccessfulJobsExpireButFailuresAreKept(t *testing.T) {
	m, _ := manager(t)
	quick, _ := m.Start("srv", "pull", "ok", nil, Options{SuccessTTL: 30 * time.Millisecond}, func(context.Context, Update) error { return nil })
	failed, _ := m.Start("srv", "pull", "bad", nil, Options{SuccessTTL: 30 * time.Millisecond}, func(_ context.Context, update Update) error {
		update(func(j *Job) { j.Failed = 1 })

		return nil
	})
	waitFor(t, "finish", func() bool { j, _ := m.Get("srv", quick.ID); return j.Status == Finished })
	waitFor(t, "finish", func() bool { j, _ := m.Get("srv", failed.ID); return j.Status == Finished })

	if j, _ := m.Get("srv", quick.ID); j.ExpiresAt == nil {
		t.Error("no expiry on a job that went well")
	}
	if j, _ := m.Get("srv", failed.ID); j.ExpiresAt != nil {
		t.Error("a job with failures expires")
	}
	time.Sleep(60 * time.Millisecond)
	m.mu.Lock()
	m.prune()
	m.mu.Unlock()
	if _, ok := m.Get("srv", quick.ID); ok {
		t.Error("the quick job is still there")
	}
	if _, ok := m.Get("srv", failed.ID); !ok {
		t.Error("the failed job is gone")
	}
}

func TestPollFollowsAValueUntilStopped(t *testing.T) {
	var mu sync.Mutex
	value, seen := int64(0), int64(0)
	stop := Poll(context.Background(), 5*time.Millisecond, func() int64 { mu.Lock(); defer mu.Unlock(); value += 10; return value }, func(v int64) { mu.Lock(); seen = v; mu.Unlock() })
	time.Sleep(40 * time.Millisecond)
	stop()
	stop() // twice is fine
	mu.Lock()
	got := seen
	mu.Unlock()
	if got < 20 {
		t.Errorf("only saw %d", got)
	}
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	if seen != got {
		t.Error("kept polling after it was stopped")
	}
	mu.Unlock()
}
