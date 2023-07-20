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
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var Template = &TemplateCommand{}

type TemplateCommand struct {
	conf string
}

func (c *TemplateCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "templateConf [alluxioMasterHostname]",
		Args:  cobra.ExactArgs(1),
		Short: "Generate a config file if one doesn't exist.",
		Run: func(cmd *cobra.Command, args []string) {
			targetDir := filepath.Join(env.ConfAlluxioConfDir.EnvVar, "alluxio-site.properties")
			content := "alluxio.master.hostname=" + args[0]
			if _, err := os.Stat(targetDir); err != nil {
				if os.IsNotExist(err) {
					if err := ioutil.WriteFile(targetDir, []byte(content), 0644); err != nil {
						log.Logger.Errorln("Error writing file:", e)
						return
					}
					log.Logger.Infoln(targetDir + " is created.")
					return
				} else {
					log.Logger.Errorln("File read error:", err)
					return
				}
			} else {
				log.Logger.Warnln(targetDir + " already exists")
				return
			}
		},
	}
	return cmd
}
