//go:build windows

package capacity

import (
	"syscall"
	"unsafe"
)

var (
	kernel32                = syscall.NewLazyDLL("kernel32.dll")
	procGetDiskFreeSpaceExW = kernel32.NewProc("GetDiskFreeSpaceExW")
)

// diskUsage reports total and free bytes. Windows has no statfs, and
// this project is stdlib-only, so the call goes through kernel32
// directly rather than pulling in x/sys.
func diskUsage(path string) (total, free uint64, err error) {
	root := path
	if len(root) >= 2 && root[1] == ':' {
		// GetDiskFreeSpaceExW wants a root ("C:\"), not a deep path.
		if len(root) == 2 {
			root += "\\"
		} else {
			root = root[:3]
		}
	}
	ptr, err := syscall.UTF16PtrFromString(root)
	if err != nil {
		return 0, 0, err
	}
	var freeAvailable, totalBytes, totalFreeBytes uint64
	r1, _, callErr := procGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(ptr)),
		uintptr(unsafe.Pointer(&freeAvailable)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&totalFreeBytes)),
	)
	if r1 == 0 {
		return 0, 0, callErr
	}
	return totalBytes, freeAvailable, nil
}
