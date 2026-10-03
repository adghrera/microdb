package tier

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

// fakeS3 is an in-process S3 stand-in. It does not recompute our
// signature (that would only prove we agree with ourselves); it pins
// the parts of SigV4 that are externally specified — the algorithm,
// credential scope, signed header set, payload hash and signature
// shape — and it round-trips bytes.
type fakeS3 struct {
	t       *testing.T
	objects map[string][]byte

	lastAuth     string
	lastAmzDate  string
	lastSha256   string
	lastRawQuery string
	lastPath     string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.lastAuth = r.Header.Get("Authorization")
	f.lastAmzDate = r.Header.Get("x-amz-date")
	f.lastSha256 = r.Header.Get("x-amz-content-sha256")
	f.lastRawQuery = r.URL.RawQuery
	f.lastPath = r.URL.Path

	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.objects[strings.TrimPrefix(r.URL.Path, "/bucket/")] = body
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if r.URL.Query().Get("list-type") == "2" {
			type content struct {
				Key  string `xml:"Key"`
				Size int64  `xml:"Size"`
			}
			type result struct {
				XMLName  xml.Name  `xml:"ListBucketResult"`
				Contents []content `xml:"Contents"`
			}
			out := result{}
			for k, v := range f.objects {
				out.Contents = append(out.Contents, content{Key: k, Size: int64(len(v))})
			}
			w.Header().Set("Content-Type", "application/xml")
			xml.NewEncoder(w).Encode(out)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		body, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Write(body)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// authParts parses the externally-specified SigV4 Authorization form:
//
//	AWS4-HMAC-SHA256 Credential=<akid>/<scope>, SignedHeaders=<h>, Signature=<sig>
func authParts(t *testing.T, auth string) (scope, signedHeaders, sig string) {
	t.Helper()
	re := regexp.MustCompile(`^AWS4-HMAC-SHA256 Credential=([^/]+)/([^,]+), SignedHeaders=([^,]+), Signature=([0-9a-f]{64})$`)
	m := re.FindStringSubmatch(auth)
	if m == nil {
		t.Fatalf("Authorization header does not have the SigV4 shape: %q", auth)
	}
	return m[2], m[3], m[4]
}

func TestS3PutGetListSignsAndRoundTrips(t *testing.T) {
	fk := &fakeS3{t: t, objects: map[string][]byte{}}
	srv := httptest.NewServer(fk)
	defer srv.Close()

	tgt, err := NewS3Target(S3Options{
		Bucket:    "bucket",
		Prefix:    "tier1",
		Region:    "eu-west-1",
		Endpoint:  srv.URL,
		AccessKey: "AKIDEXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Now: func() time.Time {
			return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("segment-bytes\n")
	if err := tgt.Put("seg-1.jsonl", strings.NewReader(string(payload)), int64(len(payload))); err != nil {
		t.Fatalf("put: %v", err)
	}

	// --- externally specified parts of the signature ---
	scope, signed, sig := authParts(t, fk.lastAuth)
	if scope != "20260102/eu-west-1/s3/aws4_request" {
		t.Errorf("credential scope = %q", scope)
	}
	// content-type is signed too, because we send it: anything the
	// request carries and SigV4 can cover, it covers.
	if signed != "content-type;host;x-amz-content-sha256;x-amz-date" {
		t.Errorf("signed headers = %q", signed)
	}
	if fk.lastSha256 != unsignedPayload {
		t.Errorf("payload hash = %q, want UNSIGNED-PAYLOAD for a streaming PUT", fk.lastSha256)
	}
	if fk.lastAmzDate != "20260102T030405Z" {
		t.Errorf("x-amz-date = %q", fk.lastAmzDate)
	}
	if fk.lastPath != "/bucket/tier1/seg-1.jsonl" {
		t.Errorf("path-style addressing wrong: %s", fk.lastPath)
	}
	_ = sig

	// --- round trip ---
	rc, err := tgt.Get("seg-1.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != string(payload) {
		t.Errorf("get returned %q", got)
	}
	// A GET signs the SHA256 of the empty body, not a free pass.
	if fk.lastSha256 != emptyPayloadHash {
		t.Errorf("GET payload hash = %q, want sha256(\"\")", fk.lastSha256)
	}

	// --- list ---
	keys, err := tgt.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || !strings.HasSuffix(keys[0], "seg-1.jsonl") {
		t.Errorf("list = %v", keys)
	}
	if !strings.Contains(fk.lastRawQuery, "list-type=2") {
		t.Errorf("list query = %q", fk.lastRawQuery)
	}
	if !strings.Contains(fk.lastRawQuery, "prefix=tier1%2F") {
		t.Errorf("list must scope to the prefix, got %q", fk.lastRawQuery)
	}
}

func TestS3MissingObjectIsNotFound(t *testing.T) {
	fk := &fakeS3{t: t, objects: map[string][]byte{}}
	srv := httptest.NewServer(fk)
	defer srv.Close()
	tgt, err := NewS3Target(S3Options{
		Bucket: "bucket", Endpoint: srv.URL,
		AccessKey: "AKID", SecretKey: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tgt.Get("nope"); err != ErrNotFound {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// TestAWSEscapePinsEncoding: SigV4 requires RFC3986 escaping with
// uppercase hex and %20 for spaces; url.QueryEscape produces '+' and
// would break any signature over a query with a space in it.
func TestAWSEscapePinsEncoding(t *testing.T) {
	cases := map[string]string{
		"a b":   "a%20b",
		"a+b":   "a%2Bb",
		"a/b":   "a%2Fb",
		"a~b":   "a~b",
		"a-b_c": "a-b_c",
		"é":     "%C3%A9",
	}
	for in, want := range cases {
		if got := awsEscape(in); got != want {
			t.Errorf("awsEscape(%q) = %q, want %q", in, got, want)
		}
	}
}
