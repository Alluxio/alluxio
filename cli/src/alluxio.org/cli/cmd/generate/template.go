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

package generate

import (
	"alluxio.org/cli/env"
	"fmt"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
)

var Template = &TemplateCommand{}

type TemplateCommand struct {
	conf string
}

func (c *TemplateCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "template --conf",
		Short: "Generate a config file if one doesn't exist.",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Println("Usage: template <alluxio_master_hostname>")
				return
			}
			targetDir := env.ConfAlluxioConfDir.EnvVar + "alluxio-site.properties"
			content := "alluxio.master.hostname=" + args[0]
			if _, err := os.Stat(targetDir); err != nil {
				if os.IsNotExist(err) {
					e := ioutil.WriteFile(targetDir, []byte(content), 0644)
					if e != nil {
						fmt.Println("Error writing file:", e)
						return
					}
					fmt.Println(targetDir + " is created.")
					return
				} else {
					fmt.Println("File read error:", err)
					return
				}
			} else {
				fmt.Println(targetDir + " already exists")
				return
			}
		},
	}
	return cmd
}
