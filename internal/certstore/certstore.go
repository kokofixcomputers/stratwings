// Package certstore holds the TLS certificate that the Panel pushes to this node (Admin > Settings > Certificates, then the
// "Certificate" tab of the node). The certificate is kept on disk, so it survives restarts, and is swapped in memory without
// restarting anything: the next connection already gets the new one.
package certstore

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"emperror.dev/errors"
)

const (
	certificateFile = "certificate.pem"
	keyFile         = "private.key"
)

// Info describes a certificate without giving anything secret away.
type Info struct {
	Fingerprint string    `json:"fingerprint"`
	NotBefore   time.Time `json:"not_before"`
	NotAfter    time.Time `json:"not_after"`
	Names       []string  `json:"names"`
	// "pushed" (by the Panel), "file" (from the configured files) or "self-signed" (made here, until the Panel pushes one).
	Source string `json:"source"`
}

// Store is safe for concurrent use.
type Store struct {
	dir string

	mu   sync.RWMutex
	cert *tls.Certificate
	info Info
}

// New makes a store that keeps its certificate in dir. Nothing is read yet, see Load.
func New(dir string) *Store {
	return &Store{dir: dir}
}

// Load reads the pushed certificate from disk, if there is one. A broken pair on disk is an error and is left alone.
func (s *Store) Load() (bool, error) {
	certPEM, err := os.ReadFile(filepath.Join(s.dir, certificateFile))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "certstore: failed to read the certificate")
	}
	keyPEM, err := os.ReadFile(filepath.Join(s.dir, keyFile))
	if err != nil {
		return false, errors.Wrap(err, "certstore: failed to read the private key")
	}

	if _, err := s.install(certPEM, keyPEM, "pushed"); err != nil {
		return false, err
	}

	return true, nil
}

// Set checks a certificate (with the certificates that follow it) and its private key, keeps them on disk and starts using them.
// A pair that does not work changes nothing.
func (s *Store) Set(certPEM, keyPEM []byte) (Info, error) {
	pair, info, err := parse(certPEM, keyPEM, "pushed")
	if err != nil {
		return Info{}, err
	}
	if err := s.persist(certPEM, keyPEM); err != nil {
		return Info{}, err
	}

	s.mu.Lock()
	s.cert, s.info = pair, info
	s.mu.Unlock()

	return info, nil
}

// Use starts using a certificate that was not pushed (from files, or self signed) when there is no pushed one, and does nothing
// when there is: a pushed certificate is always the one that is preferred.
func (s *Store) Use(pair *tls.Certificate, source string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cert != nil && s.info.Source == "pushed" {
		return
	}
	info, err := describe(pair, source)
	if err != nil {
		return
	}
	s.cert, s.info = pair, info
}

func (s *Store) install(certPEM, keyPEM []byte, source string) (Info, error) {
	pair, info, err := parse(certPEM, keyPEM, source)
	if err != nil {
		return Info{}, err
	}
	s.mu.Lock()
	s.cert, s.info = pair, info
	s.mu.Unlock()

	return info, nil
}

// Has says whether there is a certificate to serve.
func (s *Store) Has() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.cert != nil
}

// Info is what is in use now.
func (s *Store) Info() (Info, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.info, s.cert != nil
}

// GetCertificate is for tls.Config. Whatever name is asked for gets the one certificate this node has.
func (s *Store) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.cert == nil {
		return nil, errors.New("certstore: there is no certificate yet")
	}

	return s.cert, nil
}

func (s *Store) persist(certPEM, keyPEM []byte) error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return errors.Wrap(err, "certstore: failed to create the directory")
	}
	// The key first, so the certificate is never on disk without it.
	for _, f := range []struct {
		name string
		data []byte
	}{{keyFile, keyPEM}, {certificateFile, certPEM}} {
		tmp := filepath.Join(s.dir, f.name+".tmp")
		if err := os.WriteFile(tmp, f.data, 0o600); err != nil {
			return errors.Wrap(err, "certstore: failed to write "+f.name)
		}
		if err := os.Rename(tmp, filepath.Join(s.dir, f.name)); err != nil {
			return errors.Wrap(err, "certstore: failed to replace "+f.name)
		}
	}

	return nil
}

func parse(certPEM, keyPEM []byte, source string) (*tls.Certificate, Info, error) {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, Info{}, errors.Wrap(err, "certstore: the certificate and the private key do not work together")
	}
	info, err := describe(&pair, source)
	if err != nil {
		return nil, Info{}, err
	}
	if !info.NotAfter.After(time.Now()) {
		return nil, Info{}, errors.New("certstore: the certificate has expired")
	}

	return &pair, info, nil
}

func describe(pair *tls.Certificate, source string) (Info, error) {
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return Info{}, errors.Wrap(err, "certstore: failed to read the certificate")
	}
	sum := sha256.Sum256(leaf.Raw)

	names := append([]string{}, leaf.DNSNames...)
	if len(names) == 0 && leaf.Subject.CommonName != "" {
		names = append(names, leaf.Subject.CommonName)
	}
	for i := range names {
		names[i] = strings.ToLower(names[i])
	}
	sort.Strings(names)

	return Info{Fingerprint: hex.EncodeToString(sum[:]), NotBefore: leaf.NotBefore, NotAfter: leaf.NotAfter, Names: names, Source: source}, nil
}

// SelfSigned makes a certificate that nothing trusts, so that a node that is set to use https starts even before the Panel has
// pushed a real one. The Panel talks to it once (with the choice of the admin to do so) to give it the real one.
func SelfSigned(host string) (*tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, err
	}
	if host == "" {
		host = "wings.invalid"
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkixName(host),
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(30 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	pair, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)

	return &pair, err
}

var (
	sharedMu sync.RWMutex
	shared   = New("")
)

// SetShared makes this the store that the routes of the API use.
func SetShared(s *Store) {
	sharedMu.Lock()
	shared = s
	sharedMu.Unlock()
}

// Shared is the store of this process.
func Shared() *Store {
	sharedMu.RLock()
	defer sharedMu.RUnlock()

	return shared
}
