package integration

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"microdb/internal/api"
	"microdb/internal/cluster"
	"microdb/internal/metrics"
	"microdb/internal/store"
)

// genTLS writes a CA and one leaf cert/key into dir and returns
// (certFile, keyFile, caFile). Leaf is valid for 127.0.0.1/localhost.
func genTLS(t *testing.T, dir string) (string, string, string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "microdb-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644); err != nil {
		t.Fatal(err)
	}
	caKeyDER, err := x509.MarshalECPrivateKey(caKey)
	if err != nil {
		t.Fatal(err)
	}
	caKeyFile := filepath.Join(dir, "ca.key")
	if err := os.WriteFile(caKeyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: caKeyDER}), 0o600); err != nil {
		t.Fatal(err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "microdb-node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	certFile := filepath.Join(dir, "node.crt")
	keyFile := filepath.Join(dir, "node.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER}), 0o644); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, caFile
}

func mustStore(t *testing.T, dir string) *store.Store {
	t.Helper()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// newTLSServer builds the full node stack (api + internal TLS policy)
// with mutual-TLS server config.
func newTLSServer(t *testing.T, addr string, st *store.Store, cl *cluster.Cluster, certFile, keyFile, caFile string) *http.Server {
	t.Helper()
	srv := api.New(addr, st, cl)
	srvTLS, err := (&cluster.TLSOptions{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}).ServerTLS()
	if err != nil {
		t.Fatal(err)
	}
	// Verify presented certs but don't demand them at handshake —
	// RequireInternalTLS enforces presence on /internal/* so the
	// public API stays reachable anonymously.
	srvTLS.ClientAuth = tls.VerifyClientCertIfGiven
	return &http.Server{Handler: api.RequireInternalTLS(srv), TLSConfig: srvTLS}
}

// newAnonHTTPSClient trusts the test CA but presents no client cert.
func newAnonHTTPSClient(t *testing.T, caFile string) *http.Client {
	t.Helper()
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("bad CA pem")
	}
	return &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
	}
}

// TestTLSMutualAuth starts a TLS node and verifies:
//  1. a peer with the CA can join over https
//  2. a client WITHOUT a cert gets 403 on /internal/*
//  3. the public API works over https without a client cert
func TestTLSMutualAuth(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := genTLS(t, dir)

	stA := mustStore(t, filepath.Join(dir, "a"))
	lnA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addrA := "https://" + lnA.Addr().String()
	tlsOpts := &cluster.TLSOptions{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}
	clA, err := cluster.New(addrA, stA, tlsOpts)
	if err != nil {
		t.Fatal(err)
	}
	srvA := newTLSServer(t, addrA, stA, clA, certFile, keyFile, caFile)
	go srvA.ServeTLS(lnA, "", "")
	defer srvA.Close()
	clA.Start()
	defer clA.Stop()
	time.Sleep(300 * time.Millisecond)

	// 1. Peer join over TLS with client cert.
	stB := mustStore(t, filepath.Join(dir, "b"))
	clB, err := cluster.New("https://127.0.0.1:1", stB, tlsOpts)
	if err != nil {
		t.Fatal(err)
	}
	if err := clB.Join(addrA); err != nil {
		t.Fatalf("TLS join failed: %v", err)
	}

	// 2. Anonymous client blocked on /internal/*.
	anon := newAnonHTTPSClient(t, caFile)
	resp, err := anon.Get(addrA + "/internal/collections")
	if err != nil {
		t.Fatalf("anon request should reach server: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("expected 403 on /internal/* without client cert, got %d", resp.StatusCode)
	}

	// 3. Public API reachable anonymously over TLS.
	resp, err = anon.Get(addrA + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("public /health over TLS should be 200, got %d", resp.StatusCode)
	}
}

// signLeaf issues a new leaf certificate for cn, signed by the CA in
// caCertFile, and writes the pair over outCert/outKey — the exact shape
// of an automated renewal (certbot, Vault, an internal CA) writing new
// files in place.
func signLeaf(t *testing.T, caCertFile, caKeyFile, cn, outCert, outKey string) {
	t.Helper()
	caPEM, err := os.ReadFile(caCertFile)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(caPEM)
	if block == nil {
		t.Fatal("bad CA cert pem")
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := os.ReadFile(caKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := pem.Decode(keyPEM)
	caKey, err := x509.ParseECPrivateKey(kb.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	// A pause so the file stamp (size+mtime) differs even on a
	// filesystem with coarse timestamp granularity.
	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(outCert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outKey, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o644); err != nil {
		t.Fatal(err)
	}
}

// peerCN performs one request and returns the server certificate's
// CommonName — what the client actually trusted on this handshake.
func peerCN(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	// A renewed certificate applies to the NEXT handshake: an existing
	// keep-alive connection keeps presenting the cert it was opened
	// with, which is correct TLS behaviour. Close idle connections so
	// each probe is a fresh handshake — same as a new client would get.
	client.CloseIdleConnections()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s -> %d", url, resp.StatusCode)
	}
	if resp.TLS == nil || len(resp.TLS.PeerCertificates) == 0 {
		t.Fatal("response carries no peer certificate")
	}
	return resp.TLS.PeerCertificates[0].Subject.CommonName
}

func counterValue(name string) int64 {
	return atomic.LoadInt64(metrics.Default.Counter(name, "counter"))
}

// TestTLSReloadsLeafCertificate: certificate renewal must be a file
// write, not a restart. The server keeps running while its cert/key
// files are replaced in place; the next handshake presents the new
// certificate, while the client (still trusting the same CA) does
// nothing differently.
func TestTLSReloadsLeafCertificate(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := genTLS(t, dir)
	caKeyFile := filepath.Join(dir, "ca.key")

	st := mustStore(t, filepath.Join(dir, "db"))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := "https://" + ln.Addr().String()
	tlsOpts := &cluster.TLSOptions{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}
	cl, err := cluster.New(addr, st, tlsOpts)
	if err != nil {
		t.Fatal(err)
	}
	srv := newTLSServer(t, addr, st, cl, certFile, keyFile, caFile)
	go srv.ServeTLS(ln, "", "")
	defer srv.Close()
	cl.Start()
	defer cl.Stop()

	client := newAnonHTTPSClient(t, caFile)
	before := counterValue("microdb_tls_cert_reloads_total")

	cn1 := peerCN(t, client, addr+"/health")
	if cn1 != "microdb-node" {
		t.Fatalf("initial certificate CN = %q", cn1)
	}
	if got := counterValue("microdb_tls_cert_reloads_total"); got != before {
		t.Fatalf("a steady handshake must not count as a reload (%d -> %d)", before, got)
	}

	// Renew: same CA, new leaf, files replaced in place.
	signLeaf(t, caFile, caKeyFile, "microdb-node-renewed", certFile, keyFile)

	cn2 := peerCN(t, client, addr+"/health")
	if cn2 != "microdb-node-renewed" {
		t.Fatalf("after renewal the server presented CN %q — certificate was not reloaded", cn2)
	}
	if got := counterValue("microdb_tls_cert_reloads_total"); got != before+1 {
		t.Errorf("reload metric = %d, want %d", got, before+1)
	}

	// A second handshake serves the same renewed cert (cached until the
	// files change again) and does not re-count a reload.
	if cn3 := peerCN(t, client, addr+"/health"); cn3 != "microdb-node-renewed" {
		t.Errorf("renewed certificate not stable: %q", cn3)
	}
	if got := counterValue("microdb_tls_cert_reloads_total"); got != before+1 {
		t.Errorf("steady handshakes reloaded again: %d", got)
	}
}
