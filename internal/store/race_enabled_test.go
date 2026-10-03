//go:build race

package store

// raceEnabled widens allocation-based assertions: the race detector
// instruments every access and inflates TotalAlloc substantially.
const raceEnabled = true
