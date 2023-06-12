/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package main

import (
	"fmt"
	"log"
	"os"

	"entrypoint/cmd"
	"entrypoint/conf"
)

func main() {
	if len(os.Args) < 2 {
		log.Println("no process name is provided")
		printUsage()
		os.Exit(1)
	}

	if err := conf.WriteConf(); err != nil {
		log.Println(fmt.Errorf("failed to write alluxio configuration: %v", err))
		os.Exit(1)
	}

	if err := cmd.LaunchProcess(os.Args); err != nil {
		log.Println(fmt.Errorf("failed to launch process: %v", err))
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: COMMAND [COMMAND_OPTIONS]")
	fmt.Println("")
	fmt.Println(" master                    \t Start Alluxio master.")
	fmt.Println(" worker                    \t Start Alluxio worker.")
	fmt.Println(" proxy                     \t Start Alluxio proxy")
	fmt.Println(" fuse ufs_address mount_point [-o option]")
	fmt.Println("                           \t Start Alluxio FUSE file system.")
	fmt.Println(" mount ufs_address mount_point [-o option]")
	fmt.Println("                           \t Mounts an UFS address to a local mount point, example options include -o attr_timeout=700 -o s3a.accessKeyId=<S3 ACCESS KEY> -o s3a.secretKey=<S3 SECRET KEY>")
}
