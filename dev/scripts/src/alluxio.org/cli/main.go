package main

import (
	"alluxio.org/cli/env"
	"fmt"
	"os"

	"alluxio.org/cli/cmd"
	"alluxio.org/cli/process"
)

func main() {
	for _, p := range []env.Process{
		process.Master,
	} {
		env.RegisterProcess(p)
	}

	if err := cmd.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
