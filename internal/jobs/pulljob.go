package jobs

import (
	"context"
	"path"
	"strings"
	"sync"
	"time"
)

// PullItem is one file to download.
type PullItem struct {
	URL       string `json:"url"`
	Directory string `json:"directory"`
	Filename  string `json:"filename"`
}

// Name is what the job shows for the file.
func (p PullItem) Name() string {
	if p.Filename != "" {
		return p.Filename
	}

	return path.Base(strings.SplitN(p.URL, "?", 2)[0])
}

// Puller downloads one file. It calls report with the bytes written and the size of the file (0 when unknown) as it goes, and has to
// stop when ctx ends.
type Puller func(ctx context.Context, item PullItem, report func(written, total int64)) error

// PullConcurrency is how many downloads a server may have at once, which is also the limit of the node.
const PullConcurrency = 3

// pullRetryWait is the pause before a failed download is tried again (times the number of the attempt).
var pullRetryWait = time.Second

// PullWork downloads a list of files, PullConcurrency at a time over all the jobs of the server (slots), and tries a failed file again
// twice. Files that still fail are listed in the job, the others are not held up by them.
func PullWork(server string, items []PullItem, pull Puller, slots *Slots) Work {
	return func(ctx context.Context, update Update) error {
		update(func(j *Job) {
			j.Status = Running
			j.Files = len(items)
		})

		var (
			mu      sync.Mutex
			next    int
			written = make([]int64, len(items))
			wg      sync.WaitGroup
		)
		workers := PullConcurrency
		if len(items) < workers {
			workers = len(items)
		}
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					mu.Lock()
					if next >= len(items) || ctx.Err() != nil {
						mu.Unlock()

						return
					}
					index := next
					next++
					mu.Unlock()

					item := items[index]
					release, err := slots.Acquire(ctx, server+":pull", PullConcurrency)
					if err != nil {
						return
					}
					update(func(j *Job) { j.Current = item.Name() })

					report := func(n, total int64) {
						// The sum is made while the job is updated, so two reports at the same moment can not leave an old total behind.
						update(func(j *Job) {
							mu.Lock()
							written[index] = n
							var sum int64
							for _, v := range written {
								sum += v
							}
							mu.Unlock()
							j.Bytes = sum
							if len(items) == 1 && total > 0 {
								j.BytesTotal = total
							}
						})
					}

					for attempt := 1; ; attempt++ {
						err = pull(ctx, item, report)
						if err == nil || ctx.Err() != nil || attempt >= 3 {
							break
						}
						select {
						case <-time.After(pullRetryWait * time.Duration(attempt)):
						case <-ctx.Done():
						}
					}
					release()
					if ctx.Err() != nil {
						return
					}

					update(func(j *Job) {
						j.Done++
						if err != nil {
							j.Failed++
							j.Failures = append(j.Failures, Failure{Path: item.Name(), Error: err.Error()})
						}
					})
				}
			}()
		}
		wg.Wait()

		return ctx.Err()
	}
}

// Poll calls set with get() every interval until ctx ends or the returned stop function is called. It is how a job follows something
// that does not report by itself, such as the progress of an archive.
func Poll(ctx context.Context, interval time.Duration, get func() int64, set func(int64)) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				set(get())
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return func() {
		select {
		case <-done:
		default:
			close(done)
		}
		<-finished
	}
}
