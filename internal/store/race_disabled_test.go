//go:build !race

package store

// raceEnabled is false outside -race runs (see race_enabled_test.go).
const raceEnabled = false
