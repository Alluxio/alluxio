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

func InitProcessCommands(rootCmd *cobra.Command) {
	processCmd := &cobra.Command{
		Use:   "process",
		Short: "Manage Alluxio processes",
	}
	rootCmd.AddCommand(processCmd)

	for _, p := range processRegistry {
		p.InitCommands(processCmd)
	}
}

type Process interface {
	Base() *BaseProcess
	InitCommands(*cobra.Command)
	SetEnvVars(*viper.Viper)
	Start() error
}

type BaseProcess struct {
	Name              string
	JavaClassName     string
	JavaOptsEnvVarKey string
	DefaultJavaOpts   string
	ProcessOutFile    string

	LoggerEnvVarKey   string
	DefaultLoggerType string

	AuditLoggerEnvVarKey   string
	DefaultAuditLoggerType string

	AttachEnvVarKey string
}

const (
	confAlluxioLoggerType = "alluxio.logger.type"
	//confAlluxioAuditLoggerType = "alluxio.audit.logger.type" // TODO: this should be alluxio.master.audit.logger.type if master
)

//// TODO: rename to setBaseJavaOpts(), let each process implement its own setJavaOpts() to append custom parts
//func (p *BaseProcess) setJavaOpts(envVar *viper.Viper) {
//	// ${defaults} (${logger}) (${audit logger}) ${ALLUXIO_JAVA_OPTS} (${user java opts})
//	javaOpts := []string{p.DefaultJavaOpts}
//	if p.LoggerEnvVarKey != "" {
//		envVar.SetDefault(p.LoggerEnvVarKey, p.DefaultLoggerType)
//		javaOpts = append(javaOpts, fmt.Sprintf(JavaOptFormat, confAlluxioLoggerType, envVar.Get(p.LoggerEnvVarKey)))
//	}
//	//if p.AuditLoggerEnvVarKey != "" {
//	//	envVar.SetDefault(p.AuditLoggerEnvVarKey, p.DefaultAuditLoggerType)
//	//	javaOpts = append(javaOpts, fmt.Sprintf(JavaOptFormat, confAlluxioAuditLoggerType, envVar.Get(p.AuditLoggerEnvVarKey)))
//	//}
//	javaOpts = append(javaOpts, envVar.GetString(ConfAlluxioJavaOpts.EnvVar))
//	if opts := envVar.GetString(p.JavaOptsEnvVarKey); opts != "" {
//		javaOpts = append(javaOpts, envVar.GetString(p.JavaOptsEnvVarKey))
//	}
//	combinedJavaOpts := strings.Join(javaOpts, "")
//	envVar.Set(p.JavaOptsEnvVarKey, combinedJavaOpts)
//}
