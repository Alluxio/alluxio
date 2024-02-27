package process

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var RemoveWorker = &RemoveWorkerCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "remove-worker",
		JavaClassName: names.FileSystemAdminShellJavaClass,
		Parameters:    []string{"nodes", "remove"},
	},
}

type RemoveWorkerCommand struct {
	*env.BaseJavaCommand
	workerId string
}

func (c *RemoveWorkerCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *RemoveWorkerCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-worker",
		Short: "Remove a worker from ETCD membership",
		Long: `Remove given worker from the cluster, so that clients and other workers will not consider the removed worker for services.
The worker must have been stopped before it can be safely removed from the cluster.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	}
	const name = "name"
	cmd.Flags().StringVarP(&c.workerId, name, "n", "", "Worker id")
	cmd.MarkFlagRequired(name)
	return cmd
}

func (c *RemoveWorkerCommand) Run(_ []string) error {
	return c.Base().Run([]string{"-n", c.workerId})
}
