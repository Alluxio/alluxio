package env

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var processRegistry = map[string]Process{}

func RegisterProcess(p Process) Process {
	name := p.Base().Name
	if _, ok := processRegistry[name]; ok {
		panic(fmt.Sprintf("Process %v is already registered", name))
	}
	processRegistry[name] = p
	return p
}

func InitProcessCommandTree(rootCmd *cobra.Command) {
	processCmd := &cobra.Command{
		Use:   "process",
		Short: "Manage Alluxio processes",
	}
	rootCmd.AddCommand(processCmd)

	for _, p := range processRegistry {
		p.InitCommandTree(processCmd)
	}
}

type Process interface {
	Base() *BaseProcess
	InitCommandTree(*cobra.Command)
	SetEnvVars(*viper.Viper)
	Start() error
}

type BaseProcess struct {
	Name              string
	JavaClassName     string
	JavaOptsEnvVarKey string
	DefaultJavaOpts   string
	ProcessOutFile    string

	EnableConsoleLogging bool
}
