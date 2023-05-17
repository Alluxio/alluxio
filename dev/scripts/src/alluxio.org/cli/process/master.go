package process

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var Master = &MasterProcess{
	BaseProcess: &env.BaseProcess{
		Name:              "master",
		JavaOptsEnvVarKey: "ALLUXIO_MASTER_JAVA_OPTS",

		LoggerEnvVarKey:        "ALLUXIO_MASTER_LOGGER",
		DefaultLoggerType:      "MASTER_LOGGER",
		AuditLoggerEnvVarKey:   "ALLUXIO_MASTER_AUDIT_LOGGER",
		DefaultAuditLoggerType: "MASTER_AUDIT_LOGGER",
		AttachEnvVarKey:        "ALLUXIO_MASTER_ATTACH_OPTS",
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
			return p.Launch()
		},
	}
	startCmd.Flags().BoolVar(&p.Format, "format", false, "Format master")
	masterCmd.AddCommand(startCmd)
}

func (p *MasterProcess) Launch() error {
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
	log.Logger.Info("Running master")
	return nil
}
