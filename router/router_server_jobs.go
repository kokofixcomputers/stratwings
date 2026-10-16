package router

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/pterodactyl/wings/internal/importer"
	"github.com/pterodactyl/wings/internal/jobs"
	"github.com/pterodactyl/wings/internal/progress"
	"github.com/pterodactyl/wings/router/downloader"
	"github.com/pterodactyl/wings/server"
	"github.com/pterodactyl/wings/server/filesystem"
)

// maxPullFiles is the most files one download job takes.
const maxPullFiles = 1000

var pullSlots = jobs.NewSlots()

func jobManager(c *gin.Context) *jobs.Manager {
	m := jobs.Shared()
	if m == nil {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "Background jobs are not available on this node."})
	}

	return m
}

// startJob starts the job and answers with it, or says why not.
func startJob(c *gin.Context, s *server.Server, typ, title string, params map[string]any, opts jobs.Options, work jobs.Work) {
	m := jobManager(c)
	if m == nil {
		return
	}
	job, err := m.Start(s.ID(), typ, title, params, opts, work)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "This server is already doing the same kind of job, wait for it to finish."})

		return
	}
	s.Log().WithField("job", job.ID).WithField("type", typ).Info("started a background job")

	c.JSON(http.StatusAccepted, job)
}

// The jobs of a server, the newest first.
func getServerJobs(c *gin.Context) {
	m := jobManager(c)
	if m == nil {
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": m.List(ExtractServer(c).ID())})
}

func getServerJob(c *gin.Context) {
	m := jobManager(c)
	if m == nil {
		return
	}
	job, ok := m.Get(ExtractServer(c).ID(), c.Param("job"))
	if !ok {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "There is no such job."})

		return
	}
	c.JSON(http.StatusOK, job)
}

// Cancels a job that is going; one that is over is forgotten.
func deleteServerJob(c *gin.Context) {
	m := jobManager(c)
	if m == nil {
		return
	}
	id, server := c.Param("job"), ExtractServer(c).ID()
	if _, ok := m.Get(server, id); !ok {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "There is no such job."})

		return
	}
	if m.Cancel(server, id) {
		c.Status(http.StatusAccepted)

		return
	}
	m.Remove(server, id)
	c.Status(http.StatusNoContent)
}

// ---- import ---------------------------------------------------------------------------------------------------------------

// Starts copying a remote folder in the background. The connection is made first, so a wrong login or address is answered at once.
func postServerJobImport(c *gin.Context) {
	s := ExtractServer(c)
	var req importRequest
	if err := c.BindJSON(&req); err != nil {
		return
	}
	if !importable(c, s) {
		return
	}
	m := jobManager(c)
	if m == nil {
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Minute)
	defer cancel()
	src, err := importer.Connect(ctx, req.config())
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": importMessage(err)})

		return
	}

	remote := req.Path
	if remote == "" {
		remote = "/"
	}
	dest := req.Directory
	if dest == "" {
		dest = "/"
	}
	port := req.Port
	params := map[string]any{"protocol": req.Protocol, "host": req.Host, "port": port, "path": remote, "directory": dest, "overwrite": req.Overwrite}
	title := fmt.Sprintf("Import %s from %s", remote, req.Host)

	// The work owns the connection from here and closes it. If the job is refused the connection is closed here.
	job, err := m.Start(s.ID(), "import", title, params, jobs.Options{Group: "import"},
		jobs.ImportWork(src, fsWriter{s.Filesystem()}, importer.Options{Remote: remote, Dest: dest, Overwrite: req.Overwrite}))
	if err != nil {
		_ = src.Close()
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "An import is already running for this server."})

		return
	}
	s.Log().WithField("job", job.ID).WithField("host", req.Host).WithField("protocol", req.Protocol).Info("importing files from a remote server")

	c.JSON(http.StatusAccepted, job)
}

// ---- archives -------------------------------------------------------------------------------------------------------------

func postServerJobCompress(c *gin.Context) {
	s := ExtractServer(c)
	var data struct {
		RootPath string   `json:"root"`
		Files    []string `json:"files"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}
	if len(data.Files) == 0 {
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{"error": "No files were passed through to be compressed."})

		return
	}
	if !s.Filesystem().HasSpaceAvailable(true) {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "This server does not have enough available disk space to generate a compressed archive."})

		return
	}

	root, files := data.RootPath, data.Files
	title := "Archive " + files[0]
	if len(files) > 1 {
		title = fmt.Sprintf("Archive %d items of %s", len(files), root)
	}
	startJob(c, s, "compress", title, map[string]any{"root": root, "files": files}, jobs.Options{Group: "archive"},
		func(ctx context.Context, update jobs.Update) error {
			fs := s.Filesystem()
			update(func(j *jobs.Job) { j.Status = jobs.Scanning })
			count, total, err := fs.SizeOf(ctx, root, files)
			if err != nil {
				return err
			}
			update(func(j *jobs.Job) {
				j.Status = jobs.Running
				j.Files = count
				j.BytesTotal = total
			})

			p := progress.NewProgress(uint64(total))
			stop := jobs.Poll(ctx, time.Second, func() int64 { return int64(p.Written()) }, func(n int64) {
				update(func(j *jobs.Job) { j.Bytes = n })
			})
			st, err := fs.CompressFilesWith(ctx, root, files, p)
			stop()
			if err != nil {
				return err
			}
			update(func(j *jobs.Job) {
				j.Done = j.Files
				j.Bytes = j.BytesTotal
				j.Result = map[string]any{"name": st.Name(), "directory": root, "size": st.Size()}
			})

			return nil
		})
}

func postServerJobDecompress(c *gin.Context) {
	s := ExtractServer(c)
	var data struct {
		RootPath string `json:"root"`
		File     string `json:"file"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}
	if data.File == "" {
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{"error": "No archive was passed through."})

		return
	}

	root, file := data.RootPath, data.File
	startJob(c, s, "decompress", "Extract "+path.Base(file), map[string]any{"root": root, "file": file}, jobs.Options{Group: "archive"},
		func(ctx context.Context, update jobs.Update) error {
			fs := s.Filesystem()
			update(func(j *jobs.Job) { j.Status = jobs.Scanning })
			if err := fs.SpaceAvailableForDecompression(ctx, root, file); err != nil {
				return friendlyArchiveError(err)
			}
			if st, err := fs.Stat(path.Join(root, file)); err == nil {
				update(func(j *jobs.Job) { j.BytesTotal = st.Size() })
			}
			update(func(j *jobs.Job) { j.Status = jobs.Running })

			var read atomic.Int64
			stop := jobs.Poll(ctx, time.Second, read.Load, func(n int64) {
				update(func(j *jobs.Job) { j.Bytes = n })
			})
			err := fs.DecompressFileWith(ctx, root, file, func(n int64) { read.Add(n) })
			stop()
			if err != nil {
				return friendlyArchiveError(err)
			}
			update(func(j *jobs.Job) { j.Bytes = j.BytesTotal })

			return nil
		})
}

func friendlyArchiveError(err error) error {
	switch {
	case filesystem.IsErrorCode(err, filesystem.ErrCodeUnknownArchive):
		return fmt.Errorf("the archive is in a format Wings does not understand")
	case filesystem.IsErrorCode(err, filesystem.ErrCodeDiskSpace):
		return fmt.Errorf("there is not enough disk space for this")
	case strings.Contains(err.Error(), "text file busy"):
		return fmt.Errorf("one or more files this archive is attempting to overwrite are currently in use by another process, stop the server and try again")
	}

	return err
}

// ---- downloads ------------------------------------------------------------------------------------------------------------

func postServerJobPull(c *gin.Context) {
	s := ExtractServer(c)
	var data struct {
		Files []jobs.PullItem `json:"files" binding:"required,min=1"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}
	if len(data.Files) > maxPullFiles {
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{"error": fmt.Sprintf("At most %d files at once.", maxPullFiles)})

		return
	}
	for i := range data.Files {
		u, err := url.Parse(data.Files[i].URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "That is not a valid URL: " + data.Files[i].URL})

			return
		}
		if data.Files[i].Directory == "" {
			data.Files[i].Directory = "/"
		}
	}
	if err := s.Filesystem().HasSpaceErr(true); err != nil {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "This server does not have enough available disk space."})

		return
	}

	items := data.Files
	title := "Download " + items[0].Name()
	if len(items) > 1 {
		title = fmt.Sprintf("Download %d files", len(items))
	}
	// Quick downloads that went well disappear from the list after a while: installing a mod is several of them.
	startJob(c, s, "pull", title, map[string]any{"count": len(items), "directory": items[0].Directory}, jobs.Options{SuccessTTL: 10 * time.Minute},
		jobs.PullWork(s.ID(), items, pullFromURL(s), pullSlots))
}

// pullFromURL downloads one file with the downloader of the node, which keeps the rules for remote downloads (no internal addresses,
// the disk limit, the denylist).
func pullFromURL(s *server.Server) jobs.Puller {
	return func(ctx context.Context, item jobs.PullItem, report func(written, total int64)) error {
		u, err := url.Parse(item.URL)
		if err != nil {
			return err
		}
		dl := downloader.New(s, downloader.DownloadRequest{Directory: item.Directory, URL: u, FileName: item.Filename})

		done := make(chan error, 1)
		go func() { done <- dl.Execute() }()

		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case err := <-done:
				if err != nil {
					return describeDownloadError(err)
				}
				report(dl.Written(), dl.Total())

				return nil
			case <-tick.C:
				report(dl.Written(), dl.Total())
			case <-ctx.Done():
				dl.Cancel()
				<-done

				return ctx.Err()
			}
		}
	}
}

func describeDownloadError(err error) error {
	if downloader.IsDownloadError(err) {
		if strings.Contains(err.Error(), "download request failed") {
			return fmt.Errorf("an error was encountered while trying to download this file")
		}

		return fmt.Errorf("the URL or IP address could not be resolved to a valid destination")
	}

	return err
}
