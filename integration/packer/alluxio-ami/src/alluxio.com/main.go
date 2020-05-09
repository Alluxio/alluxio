package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"alluxio.com/factory"
)

const (
	logFile = "/var/log/bootstrap/alluxioBootstrap.log"
)

var (
	debugFlag bool
)

func main() {
	if os.Args[1] != "run" {
		fmt.Println("Invalid command: ", os.Args[1])
		return
	}
	if err := configureAndStartAlluxio(os.Args); err != nil {
		fmt.Println(err)
	}
}

func configureAndStartAlluxio(args []string) error {
	runCmd := flag.NewFlagSet("run", flag.ExitOnError)
	runCmd.BoolVar(&debugFlag, "debug", false, "Enable debug logging")
	runCmd.Parse(args[2:])

	if err := os.MkdirAll("/var/log/bootstrap", os.ModePerm); err != nil {
		log.Fatal(err)
	}
	f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	log.SetOutput(f)

	var userData factory.UserData
	output, err := factory.Exec("curl", []string{"--silent", "--fail", "--show-error", "http://169.254.169.254/latest/user-data"})
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(output), &userData); err != nil {
		log.Println(err)
		return err
	}

	if !userData.CftConfigure.StartAlluxio {
		log.Println("No need to start alluxio")
		return nil
	}

	config := &userData.CftConfigure
	succeeded := false
	defer func() {
		if config.AlluxioMasterDns == "local" {
			log.Println("Single EC2 instance as Alluxio Master, no need to signal")
			return
		}
		resource := "AlluxioMasters"
		if config.AlluxioRole == factory.WorkerRole {
			resource = "AlluxioWorkers"
		}

		region, err := factory.AwsRegion()
		if err != nil {
			log.Println(err)
			return
		}
		if _, err := factory.Exec("/opt/aws/bin/cfn-signal", []string{"--success", strconv.FormatBool(succeeded), "--stack", config.CftStack, "--resource", resource, "--region", region}); err != nil {
			log.Println(err)
			return
		}
	}()

	if err := factory.ConfigureAlluxio(config); err != nil {
		log.Println(err)
		return err
	}

	if err := factory.StartAlluxio(config); err != nil {
		log.Println(err)
		return err
	}
	succeeded = true
	return nil
}
