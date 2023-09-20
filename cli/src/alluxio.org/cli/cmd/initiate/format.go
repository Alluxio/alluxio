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

package initiate

import (
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
	"alluxio.org/cli/processes"
	"alluxio.org/log"
)

var Format = &FormatCommand{}

type FormatCommand struct {
	localFileSystem bool
	skipMaster      bool
	skipWorker      bool
}

func (c *FormatCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "format",
		Args:  cobra.NoArgs,
		Short: "Format Alluxio master and all workers",
		Long: `The format command formats the Alluxio master and all its workers.

Running this command on an existing Alluxio cluster deletes everything persisted in Alluxio, including cached data and any metadata information.
Data in under storage will not be changed.

> Warning: Formatting is required when you run Alluxio for the first time.
It should only be called while the cluster is not running.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if c.localFileSystem {
				// check if alluxio.master.mount.table.root.ufs set
				if env.Env.EnvVar.GetString(env.ConfAlluxioMasterMountTableRootUfs.EnvVar) != "" {
					return nil
				}
			}

			cliPath := filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), names.BinAlluxio)

			if !c.skipWorker {
				// run cache format on workers
				workerArgs := []string{"cache", "format"}
				if err := processes.RunSshCommand(
					strings.Join(append([]string{cliPath}, workerArgs...), " "),
					processes.HostGroupWorkers); err != nil {
					return stacktrace.Propagate(err, "error formatting workers")
				}
			}

			if !c.skipMaster {
				// run journal format on masters
				journalArgs := []string{"journal", "format"}
				if env.Env.EnvVar.GetString(env.ConfAlluxioMasterJournalType.EnvVar) == "EMBEDDED" {
					if err := processes.RunSshCommand(
						strings.Join(append([]string{cliPath}, journalArgs...), " "),
						processes.HostGroupMasters); err != nil {
						return stacktrace.Propagate(err, "error formatting masters")
					}
				} else {
					if err := exec.Command(cliPath, journalArgs...).Run(); err != nil {
						return stacktrace.Propagate(err, "error formatting master")
					}
				}
			}
			log.Logger.Infof("Format successful on master and workers.")

			return nil
		},
	}
	cmd.Flags().BoolVarP(&c.localFileSystem, "localFileSystem", "s", false,
		"Only format if underfs is local and doesn't already exist")
	const skipMaster, skipWorker = "skip-master", "skip-worker"
	cmd.Flags().BoolVar(&c.skipMaster, skipMaster, false, "Skip formatting journal on all masters")
	cmd.Flags().BoolVar(&c.skipWorker, skipWorker, false, "Skip formatting cache on all workers")
	cmd.MarkFlagsMutuallyExclusive(skipMaster, skipWorker)
	return cmd
}
