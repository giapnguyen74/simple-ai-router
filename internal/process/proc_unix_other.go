//go:build unix && !linux

package process

import "syscall"

func setPdeathsig(*syscall.SysProcAttr) {}
