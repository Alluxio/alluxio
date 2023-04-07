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

package ddc

import (
	"github.com/Alluxio/alluxio/pkg/ddc/alluxio"
	"github.com/Alluxio/alluxio/pkg/ddc/base"
	"github.com/Alluxio/alluxio/pkg/ddc/configs"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"fmt"
)

type buildFunc func(id string,
	config *configs.Config,
	client client.Client,
	log logr.Logger) (engine base.Engine, err error)

var buildFuncMap map[string]buildFunc

func init() {
	buildFuncMap = map[string]buildFunc{
		"alluxio": alluxio.Build,
	}
}

/**
* Build Engine from config
 */
func CreateEngine(id string,
	config *configs.Config,
	client client.Client,
	log logr.Logger) (engine base.Engine, err error) {

	if buildeFunc, found := buildFuncMap[config.Runtime]; found {
		engine, err = buildeFunc(id, config, client, log)
	} else {
		err = fmt.Errorf("Failed to build the engine due to the type %s is not found", config.Runtime)
	}

	return
}
