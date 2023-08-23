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

	"github.com/spf13/viper"
)

const (
	JavaOptFormat = " -D%v=%v"
)

// alluxioEnvVarsTemplate lists the environment variables to include in conf/alluxio-env.sh.template
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
	EnvAlluxioAssemblyClientJar = "ALLUXIO_ASSEMBLY_CLIENT_JAR"
	EnvAlluxioAssemblyServerJar = "ALLUXIO_ASSEMBLY_SERVER_JAR"
	EnvAlluxioClientClasspath   = "ALLUXIO_CLIENT_CLASSPATH"
	EnvAlluxioServerClasspath   = "ALLUXIO_SERVER_CLASSPATH"
)

var (
	ConfAlluxioConfDir = &AlluxioConfigEnvVar{
		configKey: "alluxio.conf.dir",
		EnvVar:    "ALLUXIO_CONF_DIR",
	}
	ConfAlluxioHome = &AlluxioConfigEnvVar{
		configKey: "alluxio.home",
		EnvVar:    "ALLUXIO_HOME",
	}
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
	ConfAlluxioLogsDir = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		configKey: "alluxio.logs.dir",
		EnvVar:    "ALLUXIO_LOGS_DIR",
	})
	confAlluxioUserLogsDir = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		configKey: "alluxio.user.logs.dir",
		EnvVar:    "ALLUXIO_USER_LOGS_DIR",
	})
	ConfAlluxioJavaOpts = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_JAVA_OPTS",
	})
	confAlluxioClasspath = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_CLASSPATH",
	})
	ConfAlluxioUserJavaOpts = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_USER_JAVA_OPTS",
	})
	ConfAlluxioUserAttachOpts = RegisterTemplateEnvVar(&AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_USER_ATTACH_OPTS",
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
	ConfAlluxioMasterMountTableRootUfs = &AlluxioConfigEnvVar{
		configKey: "alluxio.master.mount.table.root.ufs",
		EnvVar:    "ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS",
	}
	ConfAlluxioMasterJournalType = &AlluxioConfigEnvVar{
		configKey: "alluxio.master.journal.type",
		EnvVar:    "ALLUXIO_MASTER_JOURNAL_TYPE",
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
