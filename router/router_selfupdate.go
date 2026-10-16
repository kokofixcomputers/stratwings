package router

import (
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/gin-gonic/gin"

	"github.com/pterodactyl/wings/internal/jobs"
	"github.com/pterodactyl/wings/internal/selfupdate"
	"github.com/pterodactyl/wings/router/middleware"
	"github.com/pterodactyl/wings/system"
)

var updating sync.Mutex

// Tells which version runs and which one the release has.
func getSelfUpdate(c *gin.Context) {
	latest, err := selfupdate.Latest(c.Request.Context(), selfupdate.Options{})
	out := gin.H{"current": system.Version, "arch": runtime.GOARCH}
	if err != nil {
		out["error"] = err.Error()
	} else {
		out["latest"] = latest
	}
	c.JSON(http.StatusOK, out)
}

// Replaces this binary with the release and starts it. The answer is sent before the restart.
func postSelfUpdate(c *gin.Context) {
	if !updating.TryLock() {
		c.JSON(http.StatusConflict, gin.H{"error": "an update is already running"})
		return
	}
	defer updating.Unlock()

	exe, err := os.Executable()
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	res, err := selfupdate.Run(c.Request.Context(), selfupdate.Options{Current: system.Version, Arch: runtime.GOARCH, Exe: exe})
	if err != nil {
		log.WithField("error", err).Error("could not update wings")
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, res)

	if res.Updated {
		log.WithField("from", res.From).WithField("to", res.To).Info("wings was updated, restarting")
		go func() {
			time.Sleep(time.Second)
			// Jobs that are going are stopped and saved first, instead of being lost in the restart.
			jobs.ShutdownShared(5 * time.Second)
			if err := selfupdate.Restart(exe); err != nil {
				log.WithField("error", err).Error("could not restart into the new binary, it is used from the next start")
			}
		}()
	}
}
