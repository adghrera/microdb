// Package secret resolves operator secrets without putting them on the
// command line.
//
// Anything passed as an argv flag is visible to every user on the box
// through `ps`, and often lands in shell history and process monitors.
// A secret therefore has three homes, checked in this order:
//
//  1. --*-file: a path whose contents are the secret (the usual choice:
//     a mounted Kubernetes secret or a 0600 file)
//  2. the environment (MICRODB_AUTH_TOKEN, ...)
//  3. the flag value itself — supported, but documented as the least
//     safe option
//
// Values are also re-read while the process runs (RefreshOnce), so
// rotating a token file does not require a restart. Go has no portable
// SIGHUP (it does not exist on Windows), so the reload is a cheap
// content poll rather than a signal handler: same effect, works
// everywhere the binary does.
package secret

import (
	"errors"
	"os"
	"strings"
)

// Source describes where one secret may come from.
type Source struct {
	// Flag is the literal value passed on the command line, if any.
	Flag string
	// File is a path whose contents (trimmed) are the secret.
	File string
	// Env names an environment variable to fall back to.
	Env string
}

// Get resolves the secret: file, then environment, then flag. An empty
// result means "no secret configured", which callers treat as their
// own default (open API, plaintext store).
func (s Source) Get() (string, error) {
	if s.File != "" {
		b, err := os.ReadFile(s.File)
		if err != nil {
			return "", errors.New("read " + s.File + ": " + err.Error())
		}
		v := strings.TrimRight(string(b), "\r\n")
		if strings.TrimSpace(v) == "" {
			return "", errors.New(s.File + " is empty")
		}
		return v, nil
	}
	if s.Env != "" {
		if v := os.Getenv(s.Env); v != "" {
			return v, nil
		}
	}
	return s.Flag, nil
}

// Snapshot is a resolved set of secrets by name.
type Snapshot map[string]string

// RefreshOnce re-reads every source and returns only the names whose
// value CHANGED since last. It owns `last` so callers can hand the same
// map in every tick — unchanged secrets are not reported, so a reload
// hook does not fire on a timer for nothing.
func RefreshOnce(srcs map[string]Source, last Snapshot) (Snapshot, error) {
	out := Snapshot{}
	var firstErr error
	for name, src := range srcs {
		v, err := src.Get()
		if err != nil {
			if firstErr == nil {
				firstErr = errors.New(name + ": " + err.Error())
			}
			continue
		}
		if last != nil && last[name] == v {
			continue
		}
		out[name] = v
	}
	return out, firstErr
}

// Apply copies a refresh into the last-seen snapshot so the next tick
// only reports real changes.
func Apply(changed Snapshot, last Snapshot) Snapshot {
	for k, v := range changed {
		last[k] = v
	}
	return last
}
