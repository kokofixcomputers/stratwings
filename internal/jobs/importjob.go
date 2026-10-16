package jobs

import (
	"context"
	"fmt"

	"github.com/pterodactyl/wings/internal/importer"
)

// ImportWork is the work of an import job: it copies the remote folder and keeps the job up to date from what the importer reports.
// The connection is closed when it is over.
func ImportWork(src importer.Source, w importer.Writer, opts importer.Options) Work {
	return func(ctx context.Context, update Update) error {
		defer src.Close()

		update(func(j *Job) { j.Status = Scanning })

		sum, err := importer.Run(ctx, src, w, opts, func(e map[string]any) {
			switch e["event"] {
			case "scanning":
				update(func(j *Job) {
					j.Scanned = intOf(e["entries"])
					j.Folders = intOf(e["folders"])
					j.ScanPath = stringOf(e["path"])
				})
			case "listing":
				update(func(j *Job) {
					j.Status = Running
					j.Files = intOf(e["files"])
					j.Dirs = intOf(e["dirs"])
					j.BytesTotal = int64Of(e["bytes"])
				})
			case "file":
				update(func(j *Job) {
					j.Status = Running
					j.Done = intOf(e["done"])
					j.Current = stringOf(e["path"])
					if b := int64Of(e["bytes"]); b > j.Bytes {
						j.Bytes = b
					}
					if e["skipped"] == true {
						j.Skipped++
					}
					if e["ok"] == false {
						j.Failed++
						j.Failures = append(j.Failures, Failure{Path: stringOf(e["path"]), Error: stringOf(e["error"])})
					}
				})
			case "progress":
				update(func(j *Job) {
					if b := int64Of(e["bytesRead"]); b > j.Bytes {
						j.Bytes = b
					}
				})
			case "warning":
				update(func(j *Job) {
					j.Warnings = append(j.Warnings, fmt.Sprintf("%s: %s", stringOf(e["path"]), stringOf(e["error"])))
				})
			}
		})
		if err != nil {
			return err
		}
		update(func(j *Job) { j.Done = sum.Done })

		return nil
	}
}

func intOf(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}

	return 0
}

func int64Of(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	}

	return 0
}

func stringOf(v any) string {
	s, _ := v.(string)

	return s
}
