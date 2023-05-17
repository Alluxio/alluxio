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
