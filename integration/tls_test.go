package integration

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"microdb/internal/api"
	"microdb/internal/cluster"
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
		IsCA:                true,
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

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "microdb-node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:   time.Now().Add(24 * time.Hour),
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
