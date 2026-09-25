package process

import "os/exec"

func setProcAttrs(*exec.Cmd) {}

// Windows has no SIGTERM; both fall back to killing the process.
func termGroup(cmd *exec.Cmd) { killGroup(cmd) }

func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		cmd.Process.Kill()
	}
}
