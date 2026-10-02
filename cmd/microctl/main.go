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
  status  --url <node-url>                health + cluster view
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
