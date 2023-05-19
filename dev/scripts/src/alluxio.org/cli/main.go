package main

import (
	"alluxio.org/cli/cmd/conf"
	"fmt"
	"os"

	"alluxio.org/cli/env"
	"alluxio.org/cli/launch"
	"alluxio.org/cli/process"
)

func main() {
	for _, p := range []env.Process{
		process.Master,
	} {
		env.RegisterProcess(p)
	}

	for _, c := range []env.Command{
		conf.GetConf,
	} {
		env.RegisterCommand(c)
	}

	if err := launch.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
