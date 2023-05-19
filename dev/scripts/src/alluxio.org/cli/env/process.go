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
