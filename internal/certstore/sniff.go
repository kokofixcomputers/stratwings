package certstore

import (
	"bufio"
	"crypto/tls"
	"net"
	"sync"
	"time"
)

// SniffListener lets one port speak both http and https: the first byte of a TLS handshake is 0x16, which no http request starts with,
// so a connection that starts with it is wrapped in TLS (with cfg) and every other one is passed on as it is. A node that has not been
// switched to https keeps working for everything that connects over http, and https works as soon as the Panel has pushed a certificate.
func SniffListener(inner net.Listener, cfg *tls.Config) net.Listener {
	l := &sniffListener{Listener: inner, cfg: cfg, conns: make(chan net.Conn), failed: make(chan error, 1), done: make(chan struct{})}
	go l.loop()

	return l
}

type sniffListener struct {
	net.Listener
	cfg    *tls.Config
	conns  chan net.Conn
	failed chan error
	done   chan struct{}
	once   sync.Once
}

func (l *sniffListener) loop() {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			select {
			case l.failed <- err:
			default:
			}

			return
		}
		// Waiting for the first byte of a connection must not hold up the ones that come after it.
		go l.classify(conn)
	}
}

func (l *sniffListener) classify(conn net.Conn) {
	reader := bufio.NewReader(conn)

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	first, err := reader.Peek(1)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		_ = conn.Close()
		return
	}

	var out net.Conn = &prefixedConn{Conn: conn, reader: reader}
	if first[0] == 0x16 {
		out = tls.Server(out, l.cfg)
	}

	select {
	case l.conns <- out:
	case <-l.done:
		_ = out.Close()
	}
}

func (l *sniffListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case err := <-l.failed:
		// Whoever calls Accept again has to be told as well.
		select {
		case l.failed <- err:
		default:
		}

		return nil, err
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *sniffListener) Close() error {
	l.once.Do(func() { close(l.done) })

	return l.Listener.Close()
}

// prefixedConn reads through the reader that already took the first byte.
type prefixedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *prefixedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }
