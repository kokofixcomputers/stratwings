package router

import (
	"context"
	"io"
	"net/http"
	"net/netip"
	"path"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/internal/importer"
	wnet "github.com/pterodactyl/wings/internal/network"
	"github.com/pterodactyl/wings/server"
	"github.com/pterodactyl/wings/server/filesystem"
)

type importRequest struct {
	Protocol    string `json:"protocol" binding:"required,oneof=sftp ftp"`
	Host        string `json:"host" binding:"required"`
	Port        int    `json:"port"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	PrivateKey  string `json:"private_key"`
	Passphrase  string `json:"passphrase"`
	TLS         bool   `json:"tls"`
	InsecureTLS bool   `json:"insecure_tls"`
	Fingerprint string `json:"fingerprint"`
	Path        string `json:"path"`
	Directory   string `json:"directory"`
	Overwrite   bool   `json:"overwrite"`
}

func (r importRequest) config() importer.Config {
	return importer.Config{
		Protocol: r.Protocol, Host: r.Host, Port: r.Port, Username: r.Username, Password: r.Password, PrivateKey: r.PrivateKey,
		Passphrase: r.Passphrase, TLS: r.TLS, InsecureTLS: r.InsecureTLS, Fingerprint: r.Fingerprint,
		// The same rule as for remote downloads: nothing on the network of the node, unless the operator allowed it.
		Allowed: func(host string, ip netip.Addr) bool {
			return !wnet.IsInternal(ip) || wnet.IsAllowed(config.Get().Api.RemoteDownloadAllowlist, host, ip)
		},
	}
}

// fsWriter lets the importer write into the files of a server.
type fsWriter struct{ fs *filesystem.Filesystem }

func (w fsWriter) CreateDirectory(p string) error {
	if p == "/" || p == "." {
		return nil
	}

	return w.fs.CreateDirectory(path.Base(p), path.Dir(p))
}

func (w fsWriter) WriteFrom(p string, r io.Reader) (int64, error) { return w.fs.WriteFrom(p, r) }

func (w fsWriter) Exists(p string) bool {
	_, err := w.fs.Stat(p)

	return err == nil
}

func (w fsWriter) Denied(p string) bool { return w.fs.IsIgnored(p) != nil }

func (w fsWriter) OutOfSpace(err error) bool {
	return filesystem.IsErrorCode(err, filesystem.ErrCodeDiskSpace)
}

func importable(c *gin.Context, s *server.Server) bool {
	if s.IsInstalling() || s.IsTransferring() || s.IsRestoring() {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "This server is busy, wait for it to finish."})

		return false
	}

	return true
}

// Connects to the remote server and lists the top of the folder, to show the person what they are about to import and the fingerprint of the
// server they have to confirm. Nothing is written.
func postServerImportTest(c *gin.Context) {
	s := ExtractServer(c)
	var req importRequest
	if err := c.BindJSON(&req); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Minute)
	defer cancel()

	src, err := importer.Connect(ctx, req.config())
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": importMessage(err)})

		return
	}
	defer src.Close()

	root := req.Path
	if root == "" {
		root = "/"
	}
	entries, err := src.List(ctx, path.Clean("/"+root))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Could not read that folder: " + err.Error()})

		return
	}

	type entry struct {
		Name string `json:"name"`
		Dir  bool   `json:"dir"`
		Size int64  `json:"size"`
	}
	out := make([]entry, 0, len(entries))
	for _, e := range entries {
		if len(out) >= 100 {
			break
		}
		out = append(out, entry{e.Name, e.Dir, e.Size})
	}
	s.Log().WithField("host", req.Host).WithField("protocol", req.Protocol).Info("tested the connection for an import")

	c.JSON(http.StatusOK, gin.H{"fingerprint": src.Fingerprint(), "entries": out, "total": len(entries)})
}

func importMessage(err error) string {
	if err == importer.ErrInternal {
		return "That address is on a private network and the node may not connect to it. The operator can allow it in the Wings configuration (remote_download_allowlist)."
	}

	return err.Error()
}
