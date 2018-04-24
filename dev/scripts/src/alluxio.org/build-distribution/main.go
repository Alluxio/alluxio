package main

import (
	"v.io/x/lib/cmdline"

	"alluxio.org/build-distribution/cmd"
)

func main() {
	cmdline.Main(cmd.Root)
}
