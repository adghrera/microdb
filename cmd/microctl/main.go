// microctl — admin CLI for microdb.
//
//	microctl backup  --dir ./data1 --out backup.jsonl
//	microctl restore --dir ./data1 --in backup.jsonl
//	microctl status  --url http://127.0.0.1:8001
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"microdb/internal/store"
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
	fs.Parse(os.Args[2:])

	switch cmd {
	case "backup":
		if *out == "" {
			fmt.Fprintln(os.Stderr, "backup requires --out <file>")
			os.Exit(2)
		}
		st, err := store.Open(*dir)
		if err != nil {
			fatal(err)
		}
		defer st.Close()
		f, err := os.Create(*out)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		if err := st.Backup(f); err != nil {
			fatal(err)
		}
		fmt.Printf("backup of %s -> %s (%d docs)\n", *dir, *out, st.DocCount())
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
	case "verify":
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

func usage() {
	fmt.Fprintln(os.Stderr, `microctl — microdb admin CLI

commands:
  backup  --dir <data-dir> --out <file>    snapshot current state to JSONL
  restore --dir <data-dir> --in <file>     apply a JSONL backup (LWW merge)
  status  --url <node-url>                health + cluster view\n  verify  --dir <data-dir>                check every log record's CRC32C (exit 1 on damage)\n  repair  --url <node-url>                force anti-entropy now: re-fetch anything this node lost
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
