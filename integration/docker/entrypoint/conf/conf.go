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

package conf

import (
	"fmt"
	"os"
)

var (
	AlluxioEnv = []string{
		"ALLUXIO_CLASSPATH",
		"ALLUXIO_HOSTNAME",
		"ALLUXIO_JARS",
		"ALLUXIO_JAVA_OPTS",
		"ALLUXIO_FUSE_JAVA_OPTS",
		"ALLUXIO_PROXY_JAVA_OPTS",
		"ALLUXIO_RAM_FOLDER",
		"ALLUXIO_WORKER_JAVA_OPTS",
		"ALLUXIO_USER_JAVA_OPTS",
	}
)

func WriteConf() error {
	envFileName := "/opt/alluxio/conf/alluxio-env.sh"
	if _, err := os.Stat(envFileName); err != nil && os.IsNotExist(err) {
		f, err := os.OpenFile(envFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Errorf("failed to create conf/alluxio-env.sh file")
			return err
		}
		defer f.Close()
		for _, key := range AlluxioEnv {
			if val := os.Getenv(key); val != "" {
				if _, err := f.WriteString(fmt.Sprintf("export %v=\"%v\"\n", key, val)); err != nil {
					fmt.Errorf("failed to write conf into conf/alluxio-env.sh file")
					return err
				}
			}
		}
	}
	return nil
}
