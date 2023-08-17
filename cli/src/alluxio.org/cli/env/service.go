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
)

var serviceRegistry = map[string]*Service{}

func RegisterService(p *Service) *Service {
	if _, ok := serviceRegistry[p.Name]; ok {
		panic(fmt.Sprintf("Service %v is already registered", p.Name))
	}
	serviceRegistry[p.Name] = p
	return p
}

func InitServiceCommandTree(rootCmd *cobra.Command) {
	for _, p := range serviceRegistry {
		p.InitCommandTree(rootCmd)
	}
}

type Service struct {
	Name        string
	Description string
	Commands    []Command
}

func (s *Service) InitCommandTree(rootCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   s.Name,
		Short: s.Description,
	}
	rootCmd.AddCommand(cmd)

	for _, c := range s.Commands {
		cmd.AddCommand(c.ToCommand())
	}
}

func (s *Service) AddCommands(c ...Command) *Service {
	s.Commands = append(s.Commands, c...)
	return s
}
