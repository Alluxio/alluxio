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

package info

import (
	"fmt"

	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Report = &ReportCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "report",
		JavaClassName: "alluxio.cli.fsadmin.FileSystemAdminShell",
	},
}

type ReportCommand struct {
	*env.BaseJavaCommand

	category string // can be empty or one of: jobservice, metrics, summary, ufs
}

func (c *ReportCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ReportCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [args]", Report.CommandName),
		Short: "Reports Alluxio running cluster information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})
	// note that the capacity category is moved into its own cache command
	var isJobservice, isMetrics, isSummary, isUfs bool
	cmd.Flags().BoolVar(&isJobservice, "job-service", false, "Job service metrics information")
	cmd.Flags().BoolVar(&isMetrics, "metrics", false, "Metrics information")
	cmd.Flags().BoolVar(&isSummary, "summary", false, "Cluster summary")
	cmd.Flags().BoolVar(&isUfs, "ufs", false, "Under storage system information")
	cmd.MarkFlagsMutuallyExclusive("job-service", "metrics", "summary", "ufs")
	if isJobservice {
		c.category = "jobservice"
	} else if isMetrics {
		c.category = "metrics"
	} else if isSummary {
		c.category = "summary"
	} else if isUfs {
		c.category = "ufs"
	} else {
		c.category = "summary"
	}

	return cmd
}

func (c *ReportCommand) Run(_ []string) error {
	// TODO: output all in a serializable format and filter/trim as specified by flags
	return c.Base().Run([]string{"report", c.category})
}
