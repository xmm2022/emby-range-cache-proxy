//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package diskfree

import "golang.org/x/sys/unix"

func FreeBytes(path string) int64 {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return -1
	}
	return int64(stat.Bavail) * int64(stat.Bsize)
}
