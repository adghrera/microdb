//go:build !windows

package capacity

import "syscall"

// diskUsage reports total and free bytes for the filesystem holding
// path, using the platform statfs (no cgo, no dependencies).
func diskUsage(path string) (total, free uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	// Blocks are signed on some platforms; a negative size is a broken
	// driver rather than a number to cast blindly.
	if st.Blocks > 0 {
		total = uint64(st.Blocks) * uint64(st.Bsize)
	}
	if st.Bavail > 0 {
		free = uint64(st.Bavail) * uint64(st.Bsize)
	}
	return total, free, nil
}
