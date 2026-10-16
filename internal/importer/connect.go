package importer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path"
	"strconv"
	"syscall"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	wnet "github.com/pterodactyl/wings/internal/network"
)

// ErrInternal is returned for an address on the network of this node, which a user may not make it connect to.
var ErrInternal = errors.New("this address is on an internal network and not allowed")

// Config is how to reach the remote server.
type Config struct {
	Protocol    string // "sftp" or "ftp"
	Host        string
	Port        int
	Username    string
	Password    string
	PrivateKey  string // sftp, PEM
	Passphrase  string // of the private key
	TLS         bool   // ftp: explicit FTPS
	InsecureTLS bool   // ftp: do not check the certificate
	// Fingerprint is the SHA256 host key the server must have (sftp). Empty accepts any, the fingerprint is then reported.
	Fingerprint string
	// Allowed tells whether the node may connect to an address: internal ones are refused unless the operator allowed them.
	Allowed func(host string, ip netip.Addr) bool
}

const dialTimeout = 20 * time.Second

func (c Config) dialer() *net.Dialer {
	return &net.Dialer{
		Timeout: dialTimeout,
		// Checked on the address that is really used, after the name was resolved, so a name that points inside does not get through.
		Control: func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip, err := netip.ParseAddr(host)
			if err != nil {
				return ErrInternal
			}
			allowed := c.Allowed
			if allowed == nil {
				allowed = func(_ string, ip netip.Addr) bool { return !wnet.IsInternal(ip) }
			}
			if !allowed(c.Host, ip) {
				return ErrInternal
			}

			return nil
		},
	}
}

// Connect opens the connection and logs in.
func Connect(ctx context.Context, cfg Config) (Source, error) {
	if cfg.Port == 0 {
		cfg.Port = map[string]int{"sftp": 22, "ftp": 21}[cfg.Protocol]
	}
	switch cfg.Protocol {
	case "sftp":
		return connectSFTP(ctx, cfg)
	case "ftp":
		return connectFTP(ctx, cfg)
	}

	return nil, fmt.Errorf("unknown protocol %q", cfg.Protocol)
}

// ---- sftp -------------------------------------------------------------------------------------------------------------------

type sftpSource struct {
	conn        *ssh.Client
	client      *sftp.Client
	fingerprint string
}

func connectSFTP(ctx context.Context, cfg Config) (Source, error) {
	var methods []ssh.AuthMethod
	if cfg.PrivateKey != "" {
		var signer ssh.Signer
		var err error
		if cfg.Passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase([]byte(cfg.PrivateKey), []byte(cfg.Passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey([]byte(cfg.PrivateKey))
		}
		if err != nil {
			return nil, fmt.Errorf("the private key could not be read: %w", err)
		}
		methods = append(methods, ssh.PublicKeys(signer))
	}
	if cfg.Password != "" {
		methods = append(methods, ssh.Password(cfg.Password), ssh.KeyboardInteractive(func(_, _ string, questions []string, _ []bool) ([]string, error) {
			answers := make([]string, len(questions))
			for i := range answers {
				answers[i] = cfg.Password
			}

			return answers, nil
		}))
	}
	if len(methods) == 0 {
		return nil, errors.New("give a password or a private key")
	}

	var seen string
	ssh1 := &ssh.ClientConfig{
		User: cfg.Username,
		Auth: methods,
		HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
			seen = ssh.FingerprintSHA256(key)
			if cfg.Fingerprint != "" && cfg.Fingerprint != seen {
				return fmt.Errorf("the host key of the server is %s, not the %s that was expected", seen, cfg.Fingerprint)
			}

			return nil
		},
		Timeout: dialTimeout,
	}

	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	raw, err := cfg.dialer().DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, unwrapDial(err)
	}
	// The handshake gets the same time as the connection.
	_ = raw.SetDeadline(time.Now().Add(dialTimeout))
	sc, chans, reqs, err := ssh.NewClientConn(raw, addr, ssh1)
	if err != nil {
		_ = raw.Close()

		return nil, err
	}
	_ = raw.SetDeadline(time.Time{})
	conn := ssh.NewClient(sc, chans, reqs)

	// Starting the SFTP subsystem waits for the other server, which can say nothing for ever.
	type started struct {
		client *sftp.Client
		err    error
	}
	startedCh := make(chan started, 1)
	go func() {
		c, err := sftp.NewClient(conn)
		startedCh <- started{c, err}
	}()
	var client *sftp.Client
	select {
	case st := <-startedCh:
		if st.err != nil {
			_ = conn.Close()

			return nil, fmt.Errorf("the server does not offer SFTP: %w", st.err)
		}
		client = st.client
	case <-time.After(dialTimeout):
		_ = conn.Close()

		return nil, errors.New("the server did not start SFTP, check that SFTP is turned on for this login")
	case <-ctx.Done():
		_ = conn.Close()

		return nil, ctx.Err()
	}

	return &sftpSource{conn: conn, client: client, fingerprint: seen}, nil
}

func (s *sftpSource) List(_ context.Context, dir string) ([]Entry, error) {
	infos, err := s.client.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, fi := range infos {
		mode := fi.Mode()
		switch {
		case mode&os.ModeSymlink != 0:
			// Links could lead anywhere on the remote server, they are left out.
		case fi.IsDir():
			out = append(out, Entry{Name: fi.Name(), Dir: true})
		case mode.IsRegular():
			out = append(out, Entry{Name: fi.Name(), Size: fi.Size()})
		}
	}

	return out, nil
}

func (s *sftpSource) Open(_ context.Context, file string) (io.ReadCloser, error) {
	return s.client.Open(file)
}

func (s *sftpSource) Concurrency() int    { return 4 }
func (s *sftpSource) Fingerprint() string { return s.fingerprint }

func (s *sftpSource) Close() error {
	_ = s.client.Close()

	return s.conn.Close()
}

// ---- ftp --------------------------------------------------------------------------------------------------------------------

type ftpSource struct {
	conn *ftp.ServerConn
}

func connectFTP(ctx context.Context, cfg Config) (Source, error) {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	d := cfg.dialer()
	opts := []ftp.DialOption{
		ftp.DialWithContext(ctx),
		ftp.DialWithTimeout(dialTimeout),
		ftp.DialWithDialFunc(func(network, address string) (net.Conn, error) {
			return d.DialContext(ctx, network, address)
		}),
	}
	if cfg.TLS {
		opts = append(opts, ftp.DialWithExplicitTLS(&tls.Config{ServerName: cfg.Host, InsecureSkipVerify: cfg.InsecureTLS})) //nolint:gosec // opt-in by the user
	}

	conn, err := ftp.Dial(addr, opts...)
	if err != nil {
		return nil, unwrapDial(err)
	}
	user, pass := cfg.Username, cfg.Password
	if user == "" {
		user, pass = "anonymous", "anonymous@"
	}
	if err := conn.Login(user, pass); err != nil {
		_ = conn.Quit()

		return nil, err
	}

	return &ftpSource{conn: conn}, nil
}

func (f *ftpSource) List(_ context.Context, dir string) ([]Entry, error) {
	entries, err := f.conn.List(dir)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, e := range entries {
		if e.Name == "." || e.Name == ".." {
			continue
		}
		switch e.Type {
		case ftp.EntryTypeFolder:
			out = append(out, Entry{Name: path.Base(e.Name), Dir: true})
		case ftp.EntryTypeFile:
			out = append(out, Entry{Name: path.Base(e.Name), Size: int64(e.Size)})
		}
	}

	return out, nil
}

func (f *ftpSource) Open(_ context.Context, file string) (io.ReadCloser, error) {
	return f.conn.Retr(file)
}

// One connection can only transfer one file at a time.
func (f *ftpSource) Concurrency() int    { return 1 }
func (f *ftpSource) Fingerprint() string { return "" }
func (f *ftpSource) Close() error        { return f.conn.Quit() }

// unwrapDial turns the error of a refused address into words, the rest passes as it is.
func unwrapDial(err error) error {
	if errors.Is(err, ErrInternal) {
		return ErrInternal
	}

	return err
}
