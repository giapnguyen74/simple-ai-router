package process

import "syscall"

// setPdeathsig kills the child if the router dies without cleaning up.
func setPdeathsig(attr *syscall.SysProcAttr) { attr.Pdeathsig = syscall.SIGKILL }
