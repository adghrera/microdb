package tier

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

// S3Options configures an S3-compatible target.
//
// Credentials come from the environment when unset:
// AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, region from
// AWS_REGION. Endpoint is set for S3-compatible servers (MinIO,
// Ceph RGW, ...) and switches to path-style addressing.
type S3Options struct {
	Bucket    string
	Prefix    string
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string
	Client    *http.Client
	Now       func() time.Time
}

// S3Target talks S3's REST API over net/http, signed with AWS
// Signature V4. No SDK, no dependency — which is the point for a
// stdlib-only project.
type S3Target struct {
	opt    S3Options
	client *http.Client
}

// NewS3Target validates configuration up front: a target that cannot
// ever sign must fail at construction, not on the first archive.
func NewS3Target(o S3Options) (*S3Target, error) {
	if o.Bucket == "" {
		return nil, fmt.Errorf("tier: s3 target requires a bucket")
	}
	if o.Region == "" {
		o.Region = "us-east-1"
	}
	if o.AccessKey == "" {
		o.AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if o.SecretKey == "" {
		o.SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if o.AccessKey == "" || o.SecretKey == "" {
		return nil, fmt.Errorf("tier: s3 credentials missing (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)")
	}
	if o.Client == nil {
		o.Client = &http.Client{Timeout: 60 * time.Second}
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &S3Target{opt: o, client: o.Client}, nil
}

func (s *S3Target) Name() string {
	if s.opt.Endpoint != "" {
		return fmt.Sprintf("s3://%s/%s (endpoint %s)", s.opt.Bucket, s.opt.Prefix, s.opt.Endpoint)
	}
	return fmt.Sprintf("s3://%s/%s (region %s)", s.opt.Bucket, s.opt.Prefix, s.opt.Region)
}

// objectURL builds the request URL: virtual-hosted style against
// AWS, path style against a custom endpoint (MinIO and friends).
func (s *S3Target) objectURL(key string) (*url.URL, error) {
	var u url.URL
	p := "/" + strings.Trim(s.opt.Prefix, "/")
	if key != "" {
		if p != "/" {
			p += "/"
		}
		p += strings.Trim(key, "/")
	}
	if s.opt.Endpoint != "" {
		base, err := url.Parse(s.opt.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("tier: bad s3 endpoint: %w", err)
		}
		u = *base
		u.Path = "/" + s.opt.Bucket + p
		return &u, nil
	}
	u.Scheme = "https"
	u.Host = s.opt.Bucket + ".s3." + s.opt.Region + ".amazonaws.com"
	u.Path = p
	return &u, nil
}

const (
	emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	// UNSIGNED-PAYLOAD is what S3 accepts for streaming uploads: it
	// lets us PUT a multi-GB archive without buffering it to hash it.
	unsignedPayload = "UNSIGNED-PAYLOAD"
	algorithm       = "AWS4-HMAC-SHA256"
	terminator      = "aws4_request"
)

// sign applies SigV4 to req. SignedHeaders are host,
// x-amz-content-sha256, x-amz-date and (when present) content-type —
// exactly the headers the request will carry, nothing else.
func (s *S3Target) sign(req *http.Request, payloadHash string) {
	now := s.opt.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	// Canonical header set: host + everything x-amz + optional
	// content-type, lowercased, sorted, values trimmed.
	type kv struct{ k, v string }
	var hdrs []kv
	appendHeader := func(k, v string) {
		v = strings.Join(strings.Fields(v), " ")
		hdrs = append(hdrs, kv{strings.ToLower(k), v})
	}
	appendHeader("host", req.Host)
	for _, k := range []string{"x-amz-content-sha256", "x-amz-date", "content-type"} {
		if v := req.Header.Get(k); v != "" {
			appendHeader(k, v)
		}
	}
	sort.Slice(hdrs, func(i, j int) bool { return hdrs[i].k < hdrs[j].k })

	var canonicalHeaders strings.Builder
	var signedNames []string
	for _, h := range hdrs {
		canonicalHeaders.WriteString(h.k + ":" + h.v + "\n")
		signedNames = append(signedNames, h.k)
	}
	signedHeaders := strings.Join(signedNames, ";")

	canonicalQuery := canonicalQueryString(req.URL)
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		canonicalQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := strings.Join([]string{dateStamp, s.opt.Region, "s3", terminator}, "/")
	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		scope,
		hexSHA256([]byte(canonicalRequest)),
	}, "\n")

	key := signingKey(s.opt.SecretKey, dateStamp, s.opt.Region)
	signature := hex.EncodeToString(hmacSHA256(key, stringToSign))

	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, s.opt.AccessKey, scope, signedHeaders, signature))
}

// canonicalQueryString sorts and RFC3986-encodes the query the way
// SigV4 requires (space is %20, never +).
func canonicalQueryString(u *url.URL) string {
	q := u.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := q[k]
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, awsEscape(k)+"="+awsEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

// awsEscape percent-encodes per RFC 3986 with uppercase hex, which
// is what SigV4's canonical form demands (url.QueryEscape emits '+'
// for space and lowercase elsewhere).
func awsEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			b.WriteString("%")
			b.WriteByte(upperHex(c >> 4))
			b.WriteByte(upperHex(c & 0x0f))
		}
	}
	return b.String()
}

func upperHex(v byte) byte {
	if v < 10 {
		return '0' + v
	}
	return 'A' + (v - 10)
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func signingKey(secret, dateStamp, region string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, "s3")
	return hmacSHA256(k, terminator)
}

// Put uploads the whole reader. The payload hash is UNSIGNED-PAYLOAD
// so archives stream straight from the reader to the socket.
func (s *S3Target) Put(key string, r io.Reader, size int64) error {
	u, err := s.objectURL(key)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, u.String(), r)
	if err != nil {
		return err
	}
	if size >= 0 {
		req.ContentLength = size
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	s.sign(req, unsignedPayload)
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("tier: put %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("tier: put %s: HTTP %d: %s", key, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (s *S3Target) Get(key string) (io.ReadCloser, error) {
	u, err := s.objectURL(key)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	s.sign(req, emptyPayloadHash)
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tier: get %s: %w", key, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrNotFound
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		return nil, fmt.Errorf("tier: get %s: HTTP %d: %s", key, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp.Body, nil
}

// listResult is the ListObjectsV2 envelope we actually read.
type listResult struct {
	XMLName   xml.Name `xml:"ListBucketResult"`
	Truncated bool     `xml:"IsTruncated"`
	NextToken string   `xml:"NextContinuationToken"`
	Contents  []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
}

// List walks the prefix with ListObjectsV2, following continuation
// tokens so a cold tier with many archives is not silently truncated.
func (s *S3Target) List() ([]string, error) {
	var keys []string
	token := ""
	for {
		u, err := s.objectURL("")
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("list-type", "2")
		if s.opt.Prefix != "" {
			q.Set("prefix", strings.Trim(s.opt.Prefix, "/")+"/")
		}
		if token != "" {
			q.Set("continuation-token", token)
		}
		u.RawQuery = q.Encode()

		req, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		s.sign(req, emptyPayloadHash)
		resp, err := s.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("tier: list: %w", err)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("tier: list: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		var res listResult
		if err := xml.Unmarshal(body, &res); err != nil {
			return nil, fmt.Errorf("tier: list: bad XML: %w", err)
		}
		for _, c := range res.Contents {
			keys = append(keys, c.Key)
		}
		if !res.Truncated || res.NextToken == "" {
			break
		}
		token = res.NextToken
	}
	sort.Strings(keys)
	return keys, nil
}

var _ = path.Join
