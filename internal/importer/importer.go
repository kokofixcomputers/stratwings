// Package importer copies the files of a remote server (SFTP or FTP) into a server of this node.
package importer

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
)

// Entry is something in a remote directory. Links and special files are never listed.
type Entry struct {
	Name string
	Dir  bool
	Size int64
}

// Source is a remote server that can be read.
type Source interface {
	// List gives what is in a remote directory (absolute, with slashes).
	List(ctx context.Context, dir string) ([]Entry, error)
	// Open reads a remote file.
	Open(ctx context.Context, file string) (io.ReadCloser, error)
	// Concurrency is how many files can be read at the same time.
	Concurrency() int
	// Fingerprint identifies the remote server (the SSH host key), empty when the protocol has none.
	Fingerprint() string
	Close() error
}

// Writer is where the files go: the file system of a server.
type Writer interface {
	CreateDirectory(p string) error
	// WriteFrom writes the whole file and counts it against the disk limit as it goes.
	WriteFrom(p string, r io.Reader) (int64, error)
	Exists(p string) bool
	// Denied tells whether the server's configuration does not allow the file to be written.
	Denied(p string) bool
	// OutOfSpace tells whether an error from WriteFrom means the disk is full.
	OutOfSpace(err error) bool
}

// Options are what to copy and where to.
type Options struct {
	// Remote is the folder on the remote server to take everything from.
	Remote string
	// Dest is the folder of the server to put it in.
	Dest string
	// Overwrite replaces files that are already there, otherwise they are kept.
	Overwrite bool
	// MaxEntries stops a listing that never ends (0: 200000).
	MaxEntries int
}

// Summary is how it went.
type Summary struct {
	Files   int   `json:"files"`
	Done    int   `json:"done"`
	Failed  int   `json:"failed"`
	Skipped int   `json:"skipped"`
	Bytes   int64 `json:"bytes"`
}

const maxDepth = 64

// ValidName tells whether a name from a remote listing is safe to use as a name in the server: a remote server must not be able to
// write outside of the folder by naming a file "..", "a/b" or with a NUL.
func ValidName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}

	return !strings.ContainsAny(name, "/\\\x00")
}

type item struct {
	rel  string // with slashes, no leading slash
	size int64
	dir  bool
}

// Run reads the tree of the remote folder, makes the folders and copies the files, and reports each step to emit (one JSON-able map each).
// It stops, with what it has done, when ctx ends.
func Run(ctx context.Context, src Source, w Writer, opts Options, emit func(map[string]any)) (Summary, error) {
	var sum Summary
	var emitMu sync.Mutex
	send := func(m map[string]any) {
		emitMu.Lock()
		defer emitMu.Unlock()
		emit(m)
	}

	limit := opts.MaxEntries
	if limit <= 0 {
		limit = 200000
	}
	remote := path.Clean("/" + strings.TrimLeft(opts.Remote, "/"))
	dest := path.Clean("/" + strings.TrimLeft(opts.Dest, "/"))

	// 1. what there is
	var items []item
	var bytes int64
	skipped := 0
	type pending struct {
		rel   string
		depth int
	}
	queue := []pending{{"", 0}}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return sum, err
		}
		cur := queue[0]
		queue = queue[1:]

		entries, err := src.List(ctx, path.Join(remote, cur.rel))
		if err != nil {
			if cur.rel == "" {
				return sum, fmt.Errorf("could not read %s: %w", remote, err)
			}
			skipped++
			send(map[string]any{"event": "warning", "path": cur.rel, "error": "could not read this folder: " + err.Error()})
			continue
		}
		for _, e := range entries {
			if !ValidName(e.Name) {
				skipped++
				continue
			}
			rel := path.Join(cur.rel, e.Name)
			if e.Dir {
				if cur.depth+1 > maxDepth {
					skipped++
					continue
				}
				items = append(items, item{rel: rel, dir: true})
				queue = append(queue, pending{rel, cur.depth + 1})
			} else {
				items = append(items, item{rel: rel, size: e.Size})
				bytes += e.Size
			}
			if len(items) > limit {
				return sum, fmt.Errorf("there are more than %d files and folders, import a smaller folder", limit)
			}
		}
		if len(items)%500 < len(entries) {
			send(map[string]any{"event": "scanning", "entries": len(items)})
		}
	}

	dirs := 0
	for _, it := range items {
		if it.dir {
			dirs++
		} else {
			sum.Files++
		}
	}
	send(map[string]any{"event": "listing", "files": sum.Files, "dirs": dirs, "bytes": bytes, "skipped": skipped})

	// 2. the folders, so empty ones exist too
	if err := w.CreateDirectory(dest); err != nil {
		return sum, fmt.Errorf("could not create %s: %w", dest, err)
	}
	for _, it := range items {
		if it.dir {
			if err := w.CreateDirectory(path.Join(dest, it.rel)); err != nil {
				send(map[string]any{"event": "warning", "path": it.rel, "error": "could not create the folder: " + err.Error()})
			}
		}
	}

	// 3. the files
	files := make([]item, 0, sum.Files)
	for _, it := range items {
		if !it.dir {
			files = append(files, it)
		}
	}

	workers := src.Concurrency()
	if workers < 1 {
		workers = 1
	}
	var mu sync.Mutex
	next := 0
	diskFull := false
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if next >= len(files) || diskFull || ctx.Err() != nil {
					mu.Unlock()
					return
				}
				index := next
				next++
				mu.Unlock()

				it := files[index]
				target := path.Join(dest, it.rel)
				ok, skip, n, err := copyOne(ctx, src, w, path.Join(remote, it.rel), target, opts.Overwrite)

				mu.Lock()
				sum.Done++
				sum.Bytes += n
				switch {
				case err != nil:
					sum.Failed++
					if w.OutOfSpace(err) {
						diskFull = true
					}
				case skip:
					sum.Skipped++
				}
				done, bytesDone := sum.Done, sum.Bytes
				mu.Unlock()

				line := map[string]any{
					"event": "file", "index": index, "done": done, "total": sum.Files, "path": it.rel,
					"ok": ok, "bytes": bytesDone, "bytesTotal": bytes,
				}
				if skip {
					line["skipped"] = true
				}
				if err != nil {
					line["error"] = describe(err, w)
				}
				send(line)
			}
		}()
	}
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return sum, err
	}
	if diskFull {
		return sum, fmt.Errorf("the server is out of disk space after %d of %d files", sum.Done, sum.Files)
	}

	return sum, nil
}

func describe(err error, w Writer) string {
	if w.OutOfSpace(err) {
		return "not enough disk space"
	}

	return err.Error()
}

// copyOne copies one file. ok is false only for a failure; skip is for a file that was left alone on purpose.
func copyOne(ctx context.Context, src Source, w Writer, from, to string, overwrite bool) (ok, skip bool, n int64, err error) {
	if w.Denied(to) {
		return false, false, 0, fmt.Errorf("this file is not allowed on this server")
	}
	if !overwrite && w.Exists(to) {
		return true, true, 0, nil
	}

	r, err := src.Open(ctx, from)
	if err != nil {
		return false, false, 0, err
	}
	defer r.Close()

	n, err = w.WriteFrom(to, &ctxReader{ctx: ctx, r: r})
	if err != nil {
		return false, false, n, err
	}

	return true, false, n, nil
}

// ctxReader stops a read as soon as the context has ended.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}

	return c.r.Read(p)
}
