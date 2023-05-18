package process

import (
	"alluxio.org/cli/env"
	"alluxio.org/log"
	"fmt"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var Master = &MasterProcess{
	BaseProcess: &env.BaseProcess{
		Name:              "master",
		JavaClassName:     "alluxio.master.AlluxioMaster",
		JavaOptsEnvVarKey: "ALLUXIO_MASTER_JAVA_OPTS",
		ProcessOutFile:    "master.out",

		LoggerEnvVarKey:   "ALLUXIO_MASTER_LOGGER",
		DefaultLoggerType: "MASTER_LOGGER",
		AttachEnvVarKey:   "ALLUXIO_MASTER_ATTACH_OPTS",
	},
}

type MasterProcess struct {
	*env.BaseProcess

	Format bool
}

func (p *MasterProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}
func (p *MasterProcess) InitCommands(processCmd *cobra.Command) {
	masterCmd := &cobra.Command{
		Use:   "master",
		Short: "Manages the Alluxio master process",
	}
	processCmd.AddCommand(masterCmd)

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the process",
		RunE: func(cmd *cobra.Command, args []string) error {
			return p.Start()
		},
	}
	startCmd.Flags().BoolVar(&p.Format, "format", false, "Format master")
	masterCmd.AddCommand(startCmd)
}

func (p *MasterProcess) SetEnvVars(envVar *viper.Viper) {
	const (
		confAlluxioLoggerType      = "alluxio.logger.type"
		confAlluxioAuditLoggerType = "alluxio.master.audit.logger.type"
	)

	// ${defaults} (${logger}) (${audit logger}) ${ALLUXIO_JAVA_OPTS} (${user java opts})
	javaOpts := []string{p.DefaultJavaOpts}
	if p.LoggerEnvVarKey != "" {
		envVar.SetDefault(p.LoggerEnvVarKey, p.DefaultLoggerType)
		javaOpts = append(javaOpts, fmt.Sprintf(env.JavaOptFormat, confAlluxioLoggerType, envVar.Get(p.LoggerEnvVarKey)))
	}
	//		AuditLoggerEnvVarKey:   "ALLUXIO_MASTER_AUDIT_LOGGER",
	//		DefaultAuditLoggerType: "MASTER_AUDIT_LOGGER",

	//if p.AuditLoggerEnvVarKey != "" {
	//	envVar.SetDefault(p.AuditLoggerEnvVarKey, p.DefaultAuditLoggerType)
	//	javaOpts = append(javaOpts, fmt.Sprintf(JavaOptFormat, confAlluxioAuditLoggerType, envVar.Get(p.AuditLoggerEnvVarKey)))
	//}
	javaOpts = append(javaOpts, envVar.GetString(env.ConfAlluxioJavaOpts.EnvVar))
	if opts := envVar.GetString(p.JavaOptsEnvVarKey); opts != "" {
		javaOpts = append(javaOpts, envVar.GetString(p.JavaOptsEnvVarKey))
	}
	combinedJavaOpts := strings.Join(javaOpts, "")
	envVar.Set(p.JavaOptsEnvVarKey, strings.TrimSpace(combinedJavaOpts))
}

func (p *MasterProcess) Start() error {
	if p.Format {
		log.Logger.Info("Running format for master")
	}
	/*
	  if [[ "$1" == "-f" ]]; then
	    ${BIN}/alluxio format
	  elif [[ `${BIN}/alluxio getConf ${ALLUXIO_MASTER_JAVA_OPTS} alluxio.master.journal.type` == "EMBEDDED" ]]; then
	    JOURNAL_DIR=`${BIN}/alluxio getConf ${ALLUXIO_MASTER_JAVA_OPTS} alluxio.master.journal.folder`
	    if [ -f "${JOURNAL_DIR}" ]; then
	      echo "Journal location ${JOURNAL_DIR} is a file not a directory. Please remove the file before retrying."
	    elif [ ! -e "${JOURNAL_DIR}" ]; then
	      ${BIN}/alluxio formatMaster
	    fi
	  fi
	*/

	/*
		echo "Starting master @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
		    (JOURNAL_BACKUP="${journal_backup}" nohup ${BIN}/launch-process master > ${ALLUXIO_LOGS_DIR}/master.out 2>&1) &
	*/
	/*
	  if [[ "${console_logging_enabled}" == "true" ]]; then
	    local tmp_val=${logger_var_value}
	    logger_var_value="Console,"
	    logger_var_value+="${tmp_val}"
	  fi

	  # Set the logging variable equal to the appropriate value
	  eval ${logger_var_name}="${logger_var_value}"
	*/
	/*
		if [[ -n ${JOURNAL_BACKUP} ]]; then
		    ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.master.journal.init.from.backup=${JOURNAL_BACKUP}"
		  fi

		  # use a default Xmx value for the master
		  local res="$(contains "${ALLUXIO_MASTER_JAVA_OPTS}" "Xmx")"
		  if [[ "${res}" -eq "0" ]]; then
		    ALLUXIO_MASTER_JAVA_OPTS+=" -Xmx8g "
		  fi
		  # use a default MetaspaceSize value for the master
		  res="$(contains "${ALLUXIO_MASTER_JAVA_OPTS}" "XX:MetaspaceSize")"
		  if [[ "${res}" -eq "0" ]]; then
		    ALLUXIO_MASTER_JAVA_OPTS+=" -XX:MetaspaceSize=256M "
		  fi

		  launch_process "${ALLUXIO_MASTER_ATTACH_OPTS}" \
		                 "${ALLUXIO_MASTER_JAVA_OPTS}" \
		                 "alluxio.master.AlluxioMaster"
	*/
	/*
		exec ${JAVA} ${attach_opts} -cp ${ALLUXIO_SERVER_CLASSPATH} ${java_opts} "${main_class}" ${@:4}
	*/

	args := []string{env.Env.EnvVar.GetString(env.ConfJava.EnvVar)}
	if attachOpts := env.Env.EnvVar.GetString(p.AttachEnvVarKey); attachOpts != "" {
		args = append(args, strings.Split(attachOpts, " ")...)
	}
	args = append(args, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioServerClasspath))
	args = append(args, strings.Split(env.Env.EnvVar.GetString(p.JavaOptsEnvVarKey), " ")...)
	args = append(args, p.JavaClassName)

	startCmd := exec.Command("nohup", args...)
	for _, k := range env.Env.EnvVar.AllKeys() {
		startCmd.Env = append(startCmd.Env, fmt.Sprintf("%s=%v", k, env.Env.EnvVar.Get(k)))
	}

	outFile := filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioLogsDir.EnvVar), p.ProcessOutFile)
	f, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return stacktrace.Propagate(err, "error opening file at %v", outFile)
	}
	startCmd.Stdout = f
	startCmd.Stderr = f

	log.Logger.Info("Running master")
	log.Logger.Debugf("%v > %v 2>&1 &", startCmd.String(), outFile)
	if err := startCmd.Start(); err != nil {
		return stacktrace.Propagate(err, "error starting master")
	}
	return nil
}
