package importer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeSource struct {
	tree    map[string][]Entry // dir -> entries
	files   map[string]string  // path -> content
	workers int
	failing map[string]bool
	mu      sync.Mutex
	opened  []string
}

func (f *fakeSource) List(_ context.Context, dir string) ([]Entry, error) {
	e, ok := f.tree[dir]
	if !ok {
		return nil, errors.New("no such directory")
	}

	return e, nil
}

func (f *fakeSource) Open(_ context.Context, file string) (io.ReadCloser, error) {
	f.mu.Lock()
	f.opened = append(f.opened, file)
	f.mu.Unlock()
	if f.failing[file] {
		return nil, errors.New("permission denied")
	}

	return io.NopCloser(strings.NewReader(f.files[file])), nil
}

func (f *fakeSource) Concurrency() int    { return f.workers }
func (f *fakeSource) Fingerprint() string { return "SHA256:fake" }
func (f *fakeSource) Close() error        { return nil }

type fakeWriter struct {
	mu     sync.Mutex
	files  map[string]string
	dirs   map[string]bool
	denied map[string]bool
	limit  int // bytes the disk takes, 0 for no limit
	used   int
}

var errDisk = errors.New("disk full")

func newWriter() *fakeWriter {
	return &fakeWriter{files: map[string]string{}, dirs: map[string]bool{}, denied: map[string]bool{}}
}

func (w *fakeWriter) CreateDirectory(p string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dirs[p] = true

	return nil
}

func (w *fakeWriter) WriteFrom(p string, r io.Reader) (int64, error) {
	b, _ := io.ReadAll(r)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.limit > 0 && w.used+len(b) > w.limit {
		return 0, errDisk
	}
	w.used += len(b)
	w.files[p] = string(b)

	return int64(len(b)), nil
}

func (w *fakeWriter) Exists(p string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.files[p]

	return ok
}

func (w *fakeWriter) Denied(p string) bool      { return w.denied[p] }
func (w *fakeWriter) OutOfSpace(err error) bool { return errors.Is(err, errDisk) }

func sample() *fakeSource {
	return &fakeSource{
		workers: 3,
		failing: map[string]bool{},
		tree: map[string][]Entry{
			"/srv":           {{Name: "server.properties", Size: 3}, {Name: "mods", Dir: true}, {Name: "empty", Dir: true}, {Name: "..", Size: 1}, {Name: "a/b", Size: 1}},
			"/srv/mods":      {{Name: "one.jar", Size: 5}, {Name: "two.jar", Size: 5}, {Name: "deep", Dir: true}},
			"/srv/mods/deep": {{Name: "three.jar", Size: 7}},
			"/srv/empty":     {},
		},
		files: map[string]string{
			"/srv/server.properties": "abc", "/srv/mods/one.jar": "11111", "/srv/mods/two.jar": "22222", "/srv/mods/deep/three.jar": "3333333",
		},
	}
}

func run(t *testing.T, src Source, w Writer, opts Options) (Summary, []map[string]any, error) {
	t.Helper()
	var lines []map[string]any
	sum, err := Run(context.Background(), src, w, opts, func(m map[string]any) { lines = append(lines, m) })

	return sum, lines, err
}

func TestValidName(t *testing.T) {
	for _, bad := range []string{"", ".", "..", "a/b", "a\\b", "a\x00b"} {
		if ValidName(bad) {
			t.Errorf("%q must not be a valid name", bad)
		}
	}
	for _, good := range []string{"a", "server.properties", ".hidden", "my mod (1).jar", "..x"} {
		if !ValidName(good) {
			t.Errorf("%q should be valid", good)
		}
	}
}

func TestCopiesTheWholeTreeIncludingEmptyFolders(t *testing.T) {
	w := newWriter()
	sum, lines, err := run(t, sample(), w, Options{Remote: "/srv", Dest: "/"})
	if err != nil {
		t.Fatal(err)
	}

	if sum.Files != 4 || sum.Done != 4 || sum.Failed != 0 {
		t.Fatalf("%+v", sum)
	}
	want := map[string]string{"/server.properties": "abc", "/mods/one.jar": "11111", "/mods/two.jar": "22222", "/mods/deep/three.jar": "3333333"}
	for p, c := range want {
		if w.files[p] != c {
			t.Errorf("%s = %q, want %q", p, w.files[p], c)
		}
	}
	for _, d := range []string{"/", "/mods", "/mods/deep", "/empty"} {
		if !w.dirs[d] {
			t.Errorf("folder %s was not made", d)
		}
	}
	if lines[len(lines)-1]["event"] != "file" {
		t.Errorf("the last line should be a file, got %v", lines[len(lines)-1])
	}
	var listing map[string]any
	for _, l := range lines {
		if l["event"] == "listing" {
			listing = l
		}
	}
	if listing["files"] != 4 || listing["dirs"] != 3 || listing["bytes"] != int64(20) || listing["skipped"] != 2 {
		t.Errorf("listing = %v", listing)
	}
}

func TestNamesThatWouldEscapeAreNeverUsed(t *testing.T) {
	w := newWriter()
	if _, _, err := run(t, sample(), w, Options{Remote: "/srv", Dest: "/world"}); err != nil {
		t.Fatal(err)
	}
	for p := range w.files {
		if !strings.HasPrefix(p, "/world/") || strings.Contains(p, "..") {
			t.Errorf("a file went to %s", p)
		}
	}
	if _, ok := w.files["/world/a/b"]; ok {
		t.Error("a name with a slash was used")
	}
}

func TestProgressCountsEveryFileOnce(t *testing.T) {
	_, lines, _ := run(t, sample(), newWriter(), Options{Remote: "/srv", Dest: "/"})

	var dones []int
	for _, l := range lines {
		if l["event"] == "file" {
			dones = append(dones, l["done"].(int))
			if l["total"] != 4 {
				t.Errorf("total = %v", l["total"])
			}
		}
	}
	sort.Ints(dones)
	if len(dones) != 4 || dones[0] != 1 || dones[3] != 4 {
		t.Errorf("done values %v", dones)
	}
}

func TestExistingFilesAreKeptUnlessOverwriting(t *testing.T) {
	w := newWriter()
	w.files["/server.properties"] = "mine"

	sum, _, err := run(t, sample(), w, Options{Remote: "/srv", Dest: "/", Overwrite: false})
	if err != nil {
		t.Fatal(err)
	}
	if w.files["/server.properties"] != "mine" || sum.Skipped != 1 {
		t.Errorf("kept = %q, summary %+v", w.files["/server.properties"], sum)
	}

	if _, _, err := run(t, sample(), w, Options{Remote: "/srv", Dest: "/", Overwrite: true}); err != nil {
		t.Fatal(err)
	}
	if w.files["/server.properties"] != "abc" {
		t.Errorf("overwritten = %q", w.files["/server.properties"])
	}
}

func TestOneFailureDoesNotStopTheRest(t *testing.T) {
	src := sample()
	src.failing["/srv/mods/two.jar"] = true
	w := newWriter()

	sum, lines, err := run(t, src, w, Options{Remote: "/srv", Dest: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Failed != 1 || sum.Done != 4 {
		t.Fatalf("%+v", sum)
	}
	if _, ok := w.files["/mods/two.jar"]; ok {
		t.Error("the failed file exists")
	}
	found := false
	for _, l := range lines {
		if l["ok"] == false && l["path"] == "mods/two.jar" && l["error"] == "permission denied" {
			found = true
		}
	}
	if !found {
		t.Error("the failure was not reported with its reason")
	}
}

func TestDeniedFilesAreNotWritten(t *testing.T) {
	w := newWriter()
	w.denied["/server.properties"] = true

	sum, _, _ := run(t, sample(), w, Options{Remote: "/srv", Dest: "/"})

	if _, ok := w.files["/server.properties"]; ok || sum.Failed != 1 {
		t.Errorf("denied file written or not counted: %+v", sum)
	}
}

func TestAFullDiskStopsTheImport(t *testing.T) {
	src := sample()
	src.workers = 1
	w := newWriter()
	w.limit = 4

	sum, lines, err := run(t, src, w, Options{Remote: "/srv", Dest: "/"})

	if err == nil || !strings.Contains(err.Error(), "out of disk space") {
		t.Fatalf("err = %v", err)
	}
	if sum.Done >= 4 {
		t.Errorf("it went on after the disk was full: %+v", sum)
	}
	last := lines[len(lines)-1]
	if last["error"] != "not enough disk space" {
		t.Errorf("last = %v", last)
	}
}

func TestACancelledImportStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Run(ctx, sample(), newWriter(), Options{Remote: "/srv", Dest: "/"}, func(map[string]any) {})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}

func TestAFolderThatCanNotBeReadIsReportedAndSkipped(t *testing.T) {
	src := sample()
	delete(src.tree, "/srv/mods/deep")
	_, lines, err := run(t, src, newWriter(), Options{Remote: "/srv", Dest: "/"})
	if err != nil {
		t.Fatal(err)
	}
	warned := false
	for _, l := range lines {
		if l["event"] == "warning" && l["path"] == "mods/deep" {
			warned = true
		}
	}
	if !warned {
		t.Error("no warning for the folder")
	}
}

func TestTheRootMustBeReadable(t *testing.T) {
	_, _, err := run(t, sample(), newWriter(), Options{Remote: "/missing", Dest: "/"})
	if err == nil || !strings.Contains(err.Error(), "could not read /missing") {
		t.Errorf("err = %v", err)
	}
}

func TestListingsThatNeverEndAreStopped(t *testing.T) {
	_, _, err := run(t, sample(), newWriter(), Options{Remote: "/srv", Dest: "/", MaxEntries: 3})
	if err == nil || !strings.Contains(err.Error(), "more than 3") {
		t.Errorf("err = %v", err)
	}
}

// slowSource has many folders and every listing takes a while, like a far away SFTP server.
type slowSource struct {
	*fakeSource
	delay time.Duration
}

func (s *slowSource) List(ctx context.Context, dir string) ([]Entry, error) {
	time.Sleep(s.delay)

	return s.fakeSource.List(ctx, dir)
}

func TestManyFoldersAreListedAtTheSameTime(t *testing.T) {
	base := &fakeSource{workers: 4, failing: map[string]bool{}, tree: map[string][]Entry{}, files: map[string]string{}}
	var root []Entry
	for i := 0; i < 120; i++ {
		name := fmt.Sprintf("d%03d", i)
		root = append(root, Entry{Name: name, Dir: true})
		base.tree["/srv/"+name] = []Entry{{Name: "f.txt", Size: 1}}
		base.files["/srv/"+name+"/f.txt"] = "x"
	}
	base.tree["/srv"] = root

	start := time.Now()
	w := newWriter()
	sum, _, err := run(t, &slowSource{fakeSource: base, delay: 20 * time.Millisecond}, w, Options{Remote: "/srv", Dest: "/"})
	took := time.Since(start)

	if err != nil || sum.Done != 120 {
		t.Fatalf("%+v %v", sum, err)
	}
	// One after the other, 121 listings of 20 ms are 2.4 s.
	if took > 1200*time.Millisecond {
		t.Errorf("listing the folders took %v, they should go side by side", took)
	}
	if len(w.files) != 120 {
		t.Errorf("%d files written", len(w.files))
	}
}

func TestTheOrderOfTheFilesIsFixed(t *testing.T) {
	_, lines, _ := run(t, sample(), newWriter(), Options{Remote: "/srv", Dest: "/"})

	// Items are sorted before they are numbered: index 0 is the first path in order.
	for _, l := range lines {
		if l["event"] == "file" && l["index"] == 0 && l["path"] != "mods/deep/three.jar" {
			t.Errorf("file 0 is %v", l["path"])
		}
	}
}

// slowRead gives its data after a while, like a big file coming in.
type slowReadSource struct {
	*fakeSource
	wait time.Duration
}

func (s *slowReadSource) Open(ctx context.Context, file string) (io.ReadCloser, error) {
	r, err := s.fakeSource.Open(ctx, file)
	if err != nil {
		return nil, err
	}
	time.Sleep(s.wait)

	return r, nil
}

func TestProgressIsReportedWhileABigFileIsComingIn(t *testing.T) {
	old := heartbeat
	heartbeat = 20 * time.Millisecond
	defer func() { heartbeat = old }()

	src := &slowReadSource{fakeSource: sample(), wait: 120 * time.Millisecond}
	src.workers = 1
	_, lines, err := run(t, src, newWriter(), Options{Remote: "/srv", Dest: "/"})
	if err != nil {
		t.Fatal(err)
	}

	beats := 0
	for _, l := range lines {
		if l["event"] == "progress" {
			beats++
			if l["bytesTotal"] != int64(20) {
				t.Errorf("bytesTotal = %v", l["bytesTotal"])
			}
		}
	}
	if beats < 3 {
		t.Errorf("only %d heartbeats during a slow copy", beats)
	}
}
