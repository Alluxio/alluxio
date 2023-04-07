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

package utils

import (
	"os"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	yaml "gopkg.in/yaml.v2"
)

var log logr.Logger

func init() {
	log = ctrl.Log.WithName("utils")
}

func ToYaml(values interface{}, file *os.File) error {
	log.V(1).Info("create yaml file", "values", values)
	data, err := yaml.Marshal(values)
	if err != nil {
		log.Error(err, "failed to marshal value", "value", values)
		return err
	}

	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		log.Error(err, "failed to write file", "data", data, "fileName", file.Name())
	}
	return err
}
