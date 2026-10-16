package certstore

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// pair makes a self signed certificate and key (PEM) for these names that lasts this long.
func pair(t *testing.T, life time.Duration, names ...string) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: names[0]},
		DNSNames:     names,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(life),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func TestSetUsesAndKeepsTheCertificate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	store := New(dir)
	certPEM, keyPEM := pair(t, time.Hour, "node.example.com", "*.example.com")

	if store.Has() {
		t.Fatal("a new store has nothing")
	}
	info, err := store.Set(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	if info.Source != "pushed" || len(info.Fingerprint) != 64 || strings.Join(info.Names, ",") != "*.example.com,node.example.com" {
		t.Fatalf("info: %+v", info)
	}

	got, err := store.GetCertificate(&tls.ClientHelloInfo{ServerName: "anything.test"})
	if err != nil || got == nil {
		t.Fatalf("every name gets the certificate of the node: %v", err)
	}

	// The private key is only readable by this process, in a folder only it can enter.
	for name, mode := range map[string]os.FileMode{"private.key": 0o600, "certificate.pem": 0o600, "": 0o700} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != mode {
			t.Errorf("%q has mode %v, want %v", name, info.Mode().Perm(), mode)
		}
	}
}

func TestACertificateSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := pair(t, time.Hour, "node.example.com")
	first := New(dir)
	pushed, err := first.Set(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}

	second := New(dir)
	loaded, err := second.Load()
	if err != nil || !loaded {
		t.Fatalf("load: %v %v", loaded, err)
	}
	info, ok := second.Info()
	if !ok || info.Fingerprint != pushed.Fingerprint || info.Source != "pushed" {
		t.Fatalf("after a restart: %+v", info)
	}

	loaded, err = New(t.TempDir()).Load()
	if err != nil || loaded {
		t.Fatalf("an empty directory is not an error: %v %v", loaded, err)
	}
}

func TestAPairThatDoesNotWorkChangesNothing(t *testing.T) {
	dir := t.TempDir()
	store := New(dir)
	goodCert, goodKey := pair(t, time.Hour, "good.example.com")
	before, _ := store.Set(goodCert, goodKey)

	otherCert, otherKey := pair(t, time.Hour, "other.example.com")
	expiredCert, expiredKey := pair(t, -time.Minute, "expired.example.com")
	for name, args := range map[string][2][]byte{
		"key of another certificate": {otherCert, goodKey},
		"garbage":                    {[]byte("nope"), []byte("nope")},
		"no key":                     {otherCert, nil},
		"expired":                    {expiredCert, expiredKey},
	} {
		if _, err := store.Set(args[0], args[1]); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	_ = otherKey

	info, _ := store.Info()
	if info.Fingerprint != before.Fingerprint {
		t.Fatal("what was in use was replaced by something that does not work")
	}
	onDisk, _ := os.ReadFile(filepath.Join(dir, "certificate.pem"))
	if !bytes.Equal(onDisk, goodCert) {
		t.Fatal("what is on disk was replaced by something that does not work")
	}
}

func TestAPushedCertificateIsPreferredOverOthers(t *testing.T) {
	store := New(t.TempDir())
	temp, err := SelfSigned("node.example.com")
	if err != nil {
		t.Fatal(err)
	}

	store.Use(temp, "self-signed")
	if info, _ := store.Info(); info.Source != "self-signed" {
		t.Fatalf("source %q", info.Source)
	}

	certPEM, keyPEM := pair(t, time.Hour, "node.example.com")
	if _, err := store.Set(certPEM, keyPEM); err != nil {
		t.Fatal(err)
	}
	// A temporary or file certificate never takes the place of a pushed one.
	store.Use(temp, "self-signed")
	if info, _ := store.Info(); info.Source != "pushed" {
		t.Fatalf("source %q", info.Source)
	}
}

func TestThereIsNoCertificateUntilThereIsOne(t *testing.T) {
	store := New(t.TempDir())

	if _, err := store.GetCertificate(&tls.ClientHelloInfo{}); err == nil {
		t.Fatal("expected an error")
	}
	if _, ok := store.Info(); ok {
		t.Fatal("no info without a certificate")
	}
}

// ---- the API ----------------------------------------------------------------------------------------------------------------

func call(t *testing.T, handler http.Handler, method, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, "/api/certificates", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)

	return rec.Code, out
}

func TestTheAPISaysWhatIsInUseAndNeverTheKey(t *testing.T) {
	store := New(t.TempDir())
	handler := Handler(store)

	code, out := call(t, handler, http.MethodGet, "")
	if code != 200 || out["present"] != false {
		t.Fatalf("empty: %d %v", code, out)
	}

	certPEM, keyPEM := pair(t, time.Hour, "node.example.com")
	body, _ := json.Marshal(map[string]string{"certificate": string(certPEM), "key": string(keyPEM)})
	code, out = call(t, handler, http.MethodPost, string(body))
	if code != 200 || out["present"] != true {
		t.Fatalf("push: %d %v", code, out)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if strings.Contains(rec.Body.String(), "PRIVATE KEY") || strings.Contains(rec.Body.String(), "BEGIN") {
		t.Fatal("the API must not give anything secret back")
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("answers are not for caches")
	}
}

func TestTheAPIRefusesWhatDoesNotWork(t *testing.T) {
	handler := Handler(New(t.TempDir()))
	certPEM, _ := pair(t, time.Hour, "a.example.com")
	_, otherKey := pair(t, time.Hour, "b.example.com")
	mismatch, _ := json.Marshal(map[string]string{"certificate": string(certPEM), "key": string(otherKey)})

	for name, tc := range map[string]struct {
		body string
		want int
	}{
		"not json":   {"nope", 400},
		"empty":      {`{}`, 422},
		"no key":     {`{"certificate":"x"}`, 422},
		"mismatch":   {string(mismatch), 422},
		"too large":  {`{"certificate":"` + strings.Repeat("a", maxBody+10) + `"}`, 400},
		"wrong type": {`{"certificate":1,"key":2}`, 400},
	} {
		if code, out := call(t, handler, http.MethodPost, tc.body); code != tc.want || out["error"] == nil {
			t.Errorf("%s: %d %v, want %d", name, code, out, tc.want)
		}
	}

	if code, _ := call(t, handler, http.MethodDelete, ""); code != 405 {
		t.Errorf("delete: %d", code)
	}
}

// ---- one port for http and https --------------------------------------------------------------------------------------------

func serve(t *testing.T, store *Store) (addr string) {
	t.Helper()

	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}, GetCertificate: store.GetCertificate}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS != nil {
			_, _ = io.WriteString(w, "secure")
		} else {
			_, _ = io.WriteString(w, "plain")
		}
	})}
	go func() { _ = server.Serve(SniffListener(inner, cfg)) }()
	t.Cleanup(func() { _ = server.Close() })

	return inner.Addr().String()
}

func get(t *testing.T, url string) (string, error) {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true}}
	res, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return string(body), nil
}

func TestHTTPAndHTTPSShareOnePort(t *testing.T) {
	store := New(t.TempDir())
	certPEM, keyPEM := pair(t, time.Hour, "127.0.0.1", "node.example.com")
	if _, err := store.Set(certPEM, keyPEM); err != nil {
		t.Fatal(err)
	}
	addr := serve(t, store)

	if body, err := get(t, "http://"+addr); err != nil || body != "plain" {
		t.Fatalf("http: %q %v", body, err)
	}
	if body, err := get(t, "https://"+addr); err != nil || body != "secure" {
		t.Fatalf("https: %q %v", body, err)
	}
}

func TestHTTPSStartsWorkingWhenTheCertificateArrivesWithoutARestart(t *testing.T) {
	store := New(t.TempDir())
	addr := serve(t, store)

	if _, err := get(t, "https://"+addr); err == nil {
		t.Fatal("https can not work before there is a certificate")
	}
	if body, err := get(t, "http://"+addr); err != nil || body != "plain" {
		t.Fatalf("http keeps working: %q %v", body, err)
	}

	certPEM, keyPEM := pair(t, time.Hour, "127.0.0.1")
	if _, err := store.Set(certPEM, keyPEM); err != nil {
		t.Fatal(err)
	}
	if body, err := get(t, "https://"+addr); err != nil || body != "secure" {
		t.Fatalf("https after the push: %q %v", body, err)
	}
}

func TestACertificateReplacedWhileRunningIsServedAtOnce(t *testing.T) {
	store := New(t.TempDir())
	first, firstKey := pair(t, time.Hour, "127.0.0.1")
	if _, err := store.Set(first, firstKey); err != nil {
		t.Fatal(err)
	}
	addr := serve(t, store)

	seen := func() string {
		conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		sum := conn.ConnectionState().PeerCertificates[0].SerialNumber.String()

		return sum
	}
	before := seen()

	second, secondKey := pair(t, 2*time.Hour, "127.0.0.1")
	if _, err := store.Set(second, secondKey); err != nil {
		t.Fatal(err)
	}
	if after := seen(); after == before {
		t.Fatal("the new certificate is not in use")
	}
}

func TestAConnectionThatSaysNothingDoesNotHoldUpTheOthers(t *testing.T) {
	store := New(t.TempDir())
	addr := serve(t, store)

	idle, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()

	if body, err := get(t, "http://"+addr); err != nil || body != "plain" {
		t.Fatalf("a request behind a silent connection: %q %v", body, err)
	}
}

func TestSomethingThatIsNeitherHTTPNorTLSIsHandledLikeAnyBadHTTP(t *testing.T) {
	store := New(t.TempDir())
	addr := serve(t, store)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("\x01\x02\x03 not a protocol\r\n\r\n"))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	reply, _ := io.ReadAll(conn)

	if !strings.Contains(string(reply), "400") {
		t.Fatalf("expected a 400 from the http server, got %q", reply)
	}
	if body, err := get(t, "http://"+addr); err != nil || body != "plain" {
		t.Fatalf("the server must still work: %q %v", body, err)
	}
}
