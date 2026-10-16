package importer

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// sftpServer starts an SSH server in this process that has the SFTP subsystem and one account, and returns its address and host key
// fingerprint.
func sftpServer(t *testing.T) (host string, port int, fingerprint string) {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() == "demo" && string(pass) == "secret" {
				return nil, nil
			}

			return nil, os.ErrPermission
		},
	}
	config.AddHostKey(signer)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })

	go func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				_, chans, reqs, err := ssh.NewServerConn(nc, config)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)
				for ch := range chans {
					if ch.ChannelType() != "session" {
						_ = ch.Reject(ssh.UnknownChannelType, "no")

						continue
					}
					channel, requests, err := ch.Accept()
					if err != nil {
						continue
					}
					go func(in <-chan *ssh.Request) {
						for req := range in {
							ok := req.Type == "subsystem" && len(req.Payload) >= 4 && string(req.Payload[4:]) == "sftp"
							_ = req.Reply(ok, nil)
							if ok {
								server, err := sftp.NewServer(channel)
								if err == nil {
									_ = server.Serve()
								}
								_ = channel.Close()
							}
						}
					}(requests)
				}
			}()
		}
	}()

	addr := l.Addr().(*net.TCPAddr)

	return "127.0.0.1", addr.Port, ssh.FingerprintSHA256(signer.PublicKey())
}

func allowLocal(string, netip.Addr) bool { return true }

func TestSFTPCopiesFilesFromARealServer(t *testing.T) {
	host, port, fp := sftpServer(t)

	remote := t.TempDir()
	if err := os.MkdirAll(filepath.Join(remote, "world", "region"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(remote, "server.properties"), []byte("motd=hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(remote, "world", "region", "r.0.0.mca"), []byte("region-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A link to something outside must not be followed.
	secret := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(secret, []byte("do not copy"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(remote, "link.txt")); err != nil {
		t.Fatal(err)
	}

	src, err := Connect(context.Background(), Config{Protocol: "sftp", Host: host, Port: port, Username: "demo", Password: "secret", Fingerprint: fp, Allowed: allowLocal})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if src.Fingerprint() != fp {
		t.Errorf("fingerprint %q, want %q", src.Fingerprint(), fp)
	}

	w := newWriter()
	sum, _, err := run(t, src, w, Options{Remote: remote, Dest: "/", Overwrite: true})
	if err != nil {
		t.Fatal(err)
	}

	if sum.Done != 2 || sum.Failed != 0 {
		t.Fatalf("%+v", sum)
	}
	if w.files["/server.properties"] != "motd=hi" || w.files["/world/region/r.0.0.mca"] != "region-data" {
		t.Errorf("files = %v", w.files)
	}
	if _, ok := w.files["/link.txt"]; ok {
		t.Error("a link was followed")
	}
}

func TestSFTPRefusesAWrongPassword(t *testing.T) {
	host, port, _ := sftpServer(t)

	_, err := Connect(context.Background(), Config{Protocol: "sftp", Host: host, Port: port, Username: "demo", Password: "wrong", Allowed: allowLocal})
	if err == nil {
		t.Fatal("logged in with a wrong password")
	}
}

func TestSFTPRefusesAHostKeyThatIsNotTheExpectedOne(t *testing.T) {
	host, port, _ := sftpServer(t)

	_, err := Connect(context.Background(), Config{Protocol: "sftp", Host: host, Port: port, Username: "demo", Password: "secret", Fingerprint: "SHA256:somebodyelse", Allowed: allowLocal})
	if err == nil || !strings.Contains(err.Error(), "host key") {
		t.Fatalf("err = %v", err)
	}
}

func TestSFTPShowsTheFingerprintWhenNoneWasGiven(t *testing.T) {
	host, port, fp := sftpServer(t)

	src, err := Connect(context.Background(), Config{Protocol: "sftp", Host: host, Port: port, Username: "demo", Password: "secret", Allowed: allowLocal})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	if src.Fingerprint() != fp {
		t.Errorf("fingerprint %q, want %q", src.Fingerprint(), fp)
	}
}

func TestInternalAddressesAreRefusedByDefault(t *testing.T) {
	host, port, _ := sftpServer(t)

	// 127.0.0.1 is the node itself: nothing a user types may make the node connect there.
	_, err := Connect(context.Background(), Config{Protocol: "sftp", Host: host, Port: port, Username: "demo", Password: "secret"})
	if err == nil || !strings.Contains(err.Error(), "internal network") {
		t.Fatalf("err = %v", err)
	}
	_, err = Connect(context.Background(), Config{Protocol: "ftp", Host: host, Port: 21, Username: "x", Password: "y"})
	if err == nil {
		t.Fatal("connected to an internal address with ftp")
	}
}

func TestAPrivateKeyThatIsGarbageIsExplained(t *testing.T) {
	_, err := Connect(context.Background(), Config{Protocol: "sftp", Host: "127.0.0.1", Port: 1, Username: "demo", PrivateKey: "not a key", Allowed: allowLocal})
	if err == nil || !strings.Contains(err.Error(), "private key") {
		t.Fatalf("err = %v", err)
	}
}

func TestSomethingToLogInWithIsNeeded(t *testing.T) {
	_, err := Connect(context.Background(), Config{Protocol: "sftp", Host: "127.0.0.1", Port: 1, Username: "demo", Allowed: allowLocal})
	if err == nil || !strings.Contains(err.Error(), "password or a private key") {
		t.Fatalf("err = %v", err)
	}
}

func TestUnknownProtocolsAreRefused(t *testing.T) {
	_, err := Connect(context.Background(), Config{Protocol: "smb", Host: "x"})
	if err == nil {
		t.Fatal("accepted smb")
	}
}
