package env

import (
	"fmt"
	"github.com/spf13/cobra"
)

var commandRegistry = map[string]Command{}

func RegisterCommand(p Command) Command {
	name := p.Base().Name
	if _, ok := commandRegistry[name]; ok {
		panic(fmt.Sprintf("Command %v is already registered", name))
	}
	commandRegistry[name] = p
	return p
}

func InitCommandTree(rootCmd *cobra.Command) {
	commandCmd := &cobra.Command{
		Use:   "cli",
		Short: "Manage Alluxio commands",
	}
	rootCmd.AddCommand(commandCmd)

	for _, p := range commandRegistry {
		p.InitCommandTree(rootCmd)
	}
}

type Command interface {
	Base() *BaseCommand
	InitCommandTree(*cobra.Command)
	Run([]string) error
}

type BaseCommand struct {
	Name          string
	JavaClassName string

	DebugMode bool
	QuietMode bool

	JavaOpts string
}
