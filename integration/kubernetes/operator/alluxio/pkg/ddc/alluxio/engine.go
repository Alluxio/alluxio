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

package alluxio

import (
	data "github.com/Alluxio/alluxio/api/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/ddc/base"
	"github.com/Alluxio/alluxio/pkg/ddc/configs"
	"github.com/Alluxio/alluxio/pkg/utils/kubeclient"

	"github.com/go-logr/logr"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

/**
* Alluxio Engine implements the Engine interface
 */
type AlluxioEngine struct {
	*base.TemplateEngine
}

/**
* build the Alluxio Engine
 */
func Build(id string, config *configs.Config, client client.Client, log logr.Logger) (base.Engine, error) {
	engine := &AlluxioEngine{}
	template := base.NewTemplateEngine(engine, id, client, config, log)
	engine.TemplateEngine = template

	err := kubeclient.EnsureNamespace(client, common.ALLUXIO_NAMESPACE)
	return engine, err
}

// ID returns the id of the ddc Engine.
func (e *AlluxioEngine) ID() string {
	return e.Id
}

// Type returns the type of the ddc Engine.
func (e *AlluxioEngine) Type() string {
	return common.ALLUXIO_RUNTIME
}

// Schedule the Kubernetes nodes to run the cache system
// 1. Order the nodes by memory capacity, from the largest
// func (e *AlluxioEngine) AssignNodesToCache(datasetUFSTotalBytes uint64) ([]*corev1.Node, error) {
// 	return nil, nil
// }

// setup the cache master
func (e *AlluxioEngine) SetupMaster() (desiredNum uint32, err error) {

	var (
		masterName = e.Config.Name + "-" + e.Type() + "-master"
		namespace  = common.ALLUXIO_NAMESPACE
	)

	master, err := e.getMasterStatefulset(masterName, namespace)

	if err != nil && apierrs.IsNotFound(err) {
		//1. Is not found error
		return e.setupMasterInernal()
	} else if err != nil {
		//2. Other errors
		return
	} else {
		//3.The master has been set up
		e.Log.V(1).Info("The master has been set.")
		desiredNum = uint32(*(master.Spec.Replicas))
	}

	return
}

// Preload the dataset in the Cache System
func (e *AlluxioEngine) Preload(dataset *data.Dataset) (err error) {
	return
}

// Is the master ready
func (e *AlluxioEngine) IsMasterReady(runtime *data.Runtime) (ready bool, err error) {
	var (
		masterName = e.Config.Name + "-" + e.Type() + "-master"
		namespace  = common.ALLUXIO_NAMESPACE
	)

	master, err := e.getMasterStatefulset(masterName, namespace)
	if err != nil {
		return
	}

	if uint32(master.Status.Replicas) == runtime.Status.DesiredMasterNumberScheduled {
		ready = true
	}

	return

}

// are the workers ready
func (e *AlluxioEngine) AreWorkersReady(runtime *data.Runtime) (ready bool, err error) {
	var (
		workerReady, fuseReady bool
		namespace              string = common.ALLUXIO_NAMESPACE
		workerName             string = e.Config.Name + "-" + e.Type() + "-worker"
		fuseName               string = e.Config.Name + "-" + e.Type() + "-fuse"
	)

	workers, err := e.getDaemonset(workerName, namespace)
	if err != nil {
		return ready, err
	}

	if workers.Status.NumberAvailable > 0 {
		if runtime.Status.DesiredWorkerNumberScheduled == uint32(workers.Status.NumberReady) {
			workerReady = true
		}
	}

	if !workerReady {
		return ready, err
	}

	fuses, err := e.getDaemonset(fuseName, namespace)
	if fuses.Status.NumberAvailable > 0 {
		if runtime.Status.DesiredFuseNumberScheduled == uint32(fuses.Status.NumberReady) {
			fuseReady = true
		}
	}

	if workerReady && fuseReady {
		ready = true
	}

	return
}
