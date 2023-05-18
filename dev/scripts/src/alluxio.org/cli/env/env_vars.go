package env

import (
	"fmt"

	"github.com/spf13/viper"
)

const (
	JavaOptFormat = " -D%v=%v"
)

var alluxioEnvVarsTemplate = map[string]*AlluxioConfigEnvVar{}

type AlluxioConfigEnvVar struct {
	EnvVar      string // environment variable read by startup script
	description string // describes the environment variable in conf/alluxio-env.sh.template file

	// optional
	configKey                 string            // corresponding property key as defined in java
	additionalAlluxioJavaOpts map[string]string // additional java opts to append if non-empty java opt is added
}

func RegisterTemplateEnvVar(v *AlluxioConfigEnvVar) *AlluxioConfigEnvVar {
	if _, ok := alluxioEnvVarsTemplate[v.EnvVar]; ok {
		panic("Environment variable already registered: " + v.EnvVar)
	}
	alluxioEnvVarsTemplate[v.EnvVar] = v
	return v
}

const (
	envVersion                  = "VERSION" // VERSION is specified by libexec/version.sh and should not be overridden by the user
	envAlluxioAssemblyClientJar = "ALLUXIO_ASSEMBLY_CLIENT_JAR"
	envAlluxioAssemblyServerJar = "ALLUXIO_ASSEMBLY_SERVER_JAR"
	EnvAlluxioClientClasspath   = "ALLUXIO_CLIENT_CLASSPATH"
	EnvAlluxioServerClasspath   = "ALLUXIO_SERVER_CLASSPATH"
)

// environment variables that belong to the templatized alluxio-env.sh
// those with configKeys set will be appended to ALLUXIO_JAVA_OPTS
var (
	confJavaHome = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		EnvVar: "JAVA_HOME",
	})
	ConfJava = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		EnvVar: "JAVA",
	})
	ConfAlluxioJavaOpts = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_JAVA_OPTS",
	})
	confAlluxioClasspath = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_CLASSPATH",
	})
	confAlluxioConfDir = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		configKey: "alluxio.conf.dir",
		EnvVar:    "ALLUXIO_CONF_DIR",
	})
	confAlluxioHome = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		configKey: "alluxio.home",
		EnvVar:    "ALLUXIO_HOME",
	})
	ConfAlluxioLogsDir = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		configKey: "alluxio.logs.dir",
		EnvVar:    "ALLUXIO_LOGS_DIR",
	})
	confAlluxioUserLogsDir = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		configKey: "alluxio.user.logs.dir",
		EnvVar:    "ALLUXIO_USER_LOGS_DIR",
	})
)

// environment variables with corresponding configKeys to append to ALLUXIO_JAVA_OPTS
var (
	confAlluxioRamFolder = &AlluxioConfigEnvVar{
		configKey: "alluxio.worker.tieredstore.level0.dirs.path",
		EnvVar:    "ALLUXIO_RAM_FOLDER",
		additionalAlluxioJavaOpts: map[string]string{
			"alluxio.worker.tieredstore.level0.alias": "MEM",
		},
	}
	confAlluxioMasterHostname = &AlluxioConfigEnvVar{
		configKey: "alluxio.master.hostname",
		EnvVar:    "ALLUXIO_MASTER_HOSTNAME",
	}
	confAlluxioMasterMountTableRootUfs = &AlluxioConfigEnvVar{
		configKey: "alluxio.master.mount.table.root.ufs",
		EnvVar:    "ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS",
	}
	confAlluxioWorkerRamdiskSize = &AlluxioConfigEnvVar{
		configKey: "alluxio.worker.ramdisk.size",
		EnvVar:    "ALLUXIO_WORKER_RAMDISK_SIZE",
	}
)

func (a *AlluxioConfigEnvVar) ToJavaOpt(env *viper.Viper, required bool) string {
	v := env.Get(a.EnvVar)
	if v == nil {
		if required {
			panic("No value set for required environment variable: " + a.EnvVar)
		}
		return ""
	}
	ret := fmt.Sprintf(JavaOptFormat, a.configKey, v)
	for k2, v2 := range a.additionalAlluxioJavaOpts {
		ret += fmt.Sprintf(JavaOptFormat, k2, v2)
	}
	return ret
}
