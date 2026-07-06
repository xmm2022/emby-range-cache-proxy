//go:build !(aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris)

package diskfree

func FreeBytes(path string) int64 {
	return -1
}
