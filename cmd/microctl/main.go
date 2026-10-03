// microctl — admin CLI for microdb.
//
//	microctl backup  --dir ./data1 --out backup.jsonl
//	microctl restore --dir ./data1 --in backup.jsonl
//	microctl status  --url http://127.0.0.1:8001
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"microdb/internal/store"
	"microdb/internal/tier"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dir := fs.String("dir", "./data", "data directory")
	out := fs.String("out", "", "output file (backup)")
	in := fs.String("in", "", "input file (restore/pitr)")
	until := fs.String("until", "", "recovery point: unix millis or RFC3339 (pitr)")
	encKey := fs.String("encryption-key", "", "64-hex-char key if the archive is encrypted (pitr)")
	url := fs.String("url", "http://127.0.0.1:8001", "node base URL (status)")
	target := fs.String("target", "", "tier target URI: dir:///path or s3://bucket/prefix (tier)")
	key := fs.String("key", "", "object key to fetch from the tier (tier)")
	backupFile := fs.String("backup", "", "backup file to check (verify)")
	fs.Parse(os.Args[2:])

	switch cmd {
	case "backup":
		if *out == "" && *target == "" {
			fmt.Fprintln(os.Stderr, "backup requires --out <file> or --target <uri>")
			os.Exit(2)
		}
		st, err := store.Open(*dir)
		if err != nil {
			fatal(err)
		}
		defer st.Close()
		// Write through a temp file so the manifest records the final
		// byte count, and so an object-store upload always has a known
		// Content-Length (chunked uploads are not portable across S3
		// implementations).
		tmp, err := os.CreateTemp("", "microdb-backup-*.jsonl")
		if err != nil {
			fatal(err)
		}
		tmpName := tmp.Name()
		defer os.Remove(tmpName)
		manifest, err := st.BackupWithManifest(tmp)
		if err != nil {
			tmp.Close()
			fatal(err)
		}
		if err := tmp.Close(); err != nil {
			fatal(err)
		}
		if fi, err := os.Stat(tmpName); err == nil {
			manifest.Bytes = fi.Size()
		}
		if *out != "" {
			if err := copyFile(tmpName, *out); err != nil {
				fatal(err)
			}
			if err := manifest.WriteManifestFile(*out); err != nil {
				fatal(err)
			}
			fmt.Println("backup of " + *dir + " -> " + *out)
			fmt.Printf("  %d docs, %d bytes, sha256 %s\n", manifest.Docs, manifest.Bytes, manifest.SHA256[:16])
			fmt.Println("  manifest: " + store.ManifestPath(*out))
			fmt.Println("  check it with: microctl verify --backup <file>")
		}
		if *target != "" {
			tgt, err := tier.Open(*target)
			if err != nil {
				fatal(err)
			}
			base := fmt.Sprintf("backup-%d", manifest.CreatedUnixM)
			if err := putFile(tgt, base+".jsonl", tmpName); err != nil {
				fatal(err)
			}
			mb, _ := json.MarshalIndent(manifest, "", "  ")
			if err := tgt.Put(base+".jsonl.manifest.json", strings.NewReader(string(mb)+"\n"), int64(len(mb)+1)); err != nil {
				fatal(err)
			}
			fmt.Println("backup of " + *dir + " -> " + tgt.Name())
			fmt.Printf("  %d docs, %d bytes, sha256 %s\n", manifest.Docs, manifest.Bytes, manifest.SHA256[:16])
			fmt.Printf("  object: %s.jsonl (+ .manifest.json)\n", base)
			fmt.Printf("  fetch with: microctl tier --target <uri> --key %s.jsonl --out <file>\n", base)
		}
	case "restore":
		if *in == "" {
			fmt.Fprintln(os.Stderr, "restore requires --in <file>")
			os.Exit(2)
		}
		st, err := store.Open(*dir)
		if err != nil {
			fatal(err)
		}
		defer st.Close()
		f, err := os.Open(*in)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		n, err := st.Restore(f)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("restored %d docs into %s\n", n, *dir)
	case "status":
		printStatus(*url)
	case "tier":
		// List or fetch what the cold tier holds. This is the recovery
		// path for history that compaction archived off the local disk.
		if *target == "" {
			fmt.Fprintln(os.Stderr, "tier requires --target <uri> (dir:///path or s3://bucket/prefix)")
			os.Exit(2)
		}
		tgt, err := tier.Open(*target)
		if err != nil {
			fatal(err)
		}
		if *key == "" {
			keys, err := tgt.List()
			if err != nil {
				fatal(err)
			}
			fmt.Printf("tier %s: %d object(s)\n", tgt.Name(), len(keys))
			for _, k := range keys {
				fmt.Println("  " + k)
			}
			if len(keys) == 0 {
				return
			}
			return
		}
		rc, err := tgt.Get(*key)
		if err != nil {
			fatal(err)
		}
		defer rc.Close()
		var w io.Writer = os.Stdout
		if *out != "" {
			f, err := os.Create(*out)
			if err != nil {
				fatal(err)
			}
			defer f.Close()
			w = f
		}
		n, err := io.Copy(w, rc)
		if err != nil {
			fatal(err)
		}
		if *out != "" {
			fmt.Printf("fetched %s -> %s (%d bytes)\n", *key, *out, n)
		}
	case "verify":
		if *backupFile != "" {
			// Backup mode: prove the file still matches the manifest it
			// was written with. An unverifiable backup is not a backup.
			rep, err := store.VerifyBackup(*backupFile)
			if err != nil {
				fatal(err)
			}
			fmt.Println("backup  " + rep.Path)
			fmt.Printf("size    %d bytes\n", rep.Bytes)
			fmt.Printf("docs    %d\n", rep.Docs)
			if rep.Manifest != nil {
				fmt.Println("created " + time.UnixMilli(rep.Manifest.CreatedUnixM).Format(time.RFC3339))
				fmt.Printf("sha256  %s (manifest %s)\n", rep.SHA256[:16], rep.Manifest.SHA256[:16])
			}
			if len(rep.Problems) == 0 && rep.Manifest != nil {
				fmt.Println("verified OK")
				return
			}
			for _, p := range rep.Problems {
				fmt.Println("PROBLEM  " + p)
			}
			os.Exit(1)
		}
		// Offline integrity scan of the commit log: every record's
		// CRC32C is checked without loading the store. Exit code 1 if
		// anything other than a torn final record is damaged, so it can
		// gate a backup or a cron.
		rep, err := store.VerifyDir(*dir)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("log      %s (%d bytes)\n", rep.Path, rep.Bytes)
		fmt.Printf("records  %d total, %d checksummed, %d legacy\n",
			rep.Records, rep.ChecksummedRecords, rep.LegacyRecords)
		if rep.Encrypted {
			fmt.Println("format   encrypted (ENC1)")
		}
		if rep.TornTail {
			fmt.Println("tail     torn final record (crash mid-append) - tolerated by replay")
		}
		if len(rep.Corrupt) > 0 {
			fmt.Printf("CORRUPT  %d record(s):\n", len(rep.Corrupt))
			for _, c := range rep.Corrupt {
				fmt.Printf("  line %d @ byte %d: %s\n", c.Line, c.Offset, c.Reason)
			}
			fmt.Println("run microctl repair --url <node> to re-fetch affected documents from peers")
		} else {
			fmt.Println("integrity OK")
		}
		if !rep.OK() {
			os.Exit(1)
		}
	case "repair":
		// Force an anti-entropy pass against every peer NOW: the
		// recovery step after verify finds damage (or after a disk is
		// replaced). Anything this node lost is re-fetched from a peer
		// that still holds it.
		req, err := http.NewRequest("POST", *url+"/internal/repair", nil)
		if err != nil {
			fatal(err)
		}
		resp, err := (&http.Client{Timeout: 10 * time.Minute}).Do(req)
		if err != nil {
			fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			fatal(fmt.Errorf("repair failed (%d): %s", resp.StatusCode, body))
		}
		fmt.Printf("repair of %s\n%s", *url, string(body))
	case "hints":
		resp, err := httpGet(*url + "/internal/hints")
		if err != nil {
			fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Println(string(body))
	case "decommission":
		// Ask the target node to drain: refuse writes, hand its data
		// off to the remaining peers, and broadcast its departure.
		// The node process itself must then be stopped by the operator.
		req, err := http.NewRequest("POST", *url+"/internal/decommission", nil)
		if err != nil {
			fatal(err)
		}
		resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
		if err != nil {
			fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			fatal(fmt.Errorf("decommission failed (%d): %s", resp.StatusCode, body))
		}
		fmt.Printf("node drained: %s\n", body)
		fmt.Println("safe to stop the microdb process now")
	case "pitr":
		// Point-in-time recovery: replay a raw commit-log archive
		// (see --archive-dir on the server) into a NEW data dir,
		// applying only records with TS <= --until.
		if *in == "" || *until == "" {
			fmt.Fprintln(os.Stderr, "pitr requires --in <raw-log> --until <RFC3339|unix-millis> --dir <new-dir>")
			os.Exit(2)
		}
		var untilMs int64
		if ms, err := strconv.ParseInt(*until, 10, 64); err == nil {
			untilMs = ms
		} else {
			t, err := time.Parse(time.RFC3339, *until)
			if err != nil {
				fatal(fmt.Errorf("--until must be unix millis or RFC3339: %w", err))
			}
			untilMs = t.UnixMilli()
		}
		f, err := os.Open(*in)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		st, n, err := store.ReplayUntilKey(f, untilMs, *dir, *encKey)
		if err != nil {
			fatal(err)
		}
		defer st.Close()
		fmt.Printf("PITR: replayed %d records (TS <= %d) into %s — %d docs live\n", n, untilMs, *dir, st.DocCount())
	default:
		usage()
		os.Exit(2)
	}
}

func printStatus(base string) {
	for _, ep := range []string{"/health", "/version", "/api/cluster"} {
		resp, err := httpGet(base + ep)
		if err != nil {
			fatal(fmt.Errorf("%s: %w", ep, err))
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Printf("%-14s %s\n", ep, string(body))
	}
}

// copyFile copies src over dst atomically enough for a backup: write
// to a temp sibling first, then rename, so a crash mid-copy cannot
// leave a half-written file that later verifies as a backup.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".part"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

// putFile streams a local file to a tier target under key.
func putFile(t tier.Target, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	return t.Put(key, f, fi.Size())
}

func usage() {
	fmt.Fprintln(os.Stderr, `microctl — microdb admin CLI

commands:
  backup  --dir <data-dir> --out <file>    snapshot current state to JSONL
  restore --dir <data-dir> --in <file>     apply a JSONL backup (LWW merge)
  status  --url <node-url>                health + cluster view\n  tier     --target <uri> [--key <k> --out <f>]  list/fetch cold-tier archives
  verify  --dir <data-dir>                check every log record's CRC32C (exit 1 on damage)\n  repair  --url <node-url>                force anti-entropy now: re-fetch anything this node lost
  hints   --url <node-url>                hinted-handoff debt (pending/delivered/dropped)
  decommission --url <node-url>          drain a node: hand off data + broadcast leave`)
}

func httpGet(url string) (*http.Response, error) {
	client := &http.Client{}
	return client.Get(url)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
