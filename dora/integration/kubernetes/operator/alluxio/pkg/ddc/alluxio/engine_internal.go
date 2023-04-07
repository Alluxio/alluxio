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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/utils"
	"github.com/Alluxio/alluxio/pkg/utils/helm"
	"github.com/Alluxio/alluxio/pkg/utils/kubeclient"
	"github.com/Alluxio/alluxio/pkg/utils/kubectl"

	"k8s.io/client-go/util/retry"
)

// setup the cache master
func (e *AlluxioEngine) setupMasterInernal() (desiredNum uint32, err error) {

	var (
		chartName     = utils.GetChartsDirectory() + "/" + common.ALLUXIO_CHART
		namespace     = common.ALLUXIO_NAMESPACE
		name          = e.Config.Name
		configMapName = name + "-" + e.Type() + "-values"
		valueFileName = ""
	)

	// 1.generate alluxio value struct
	alluxioValue, err := e.generateAlluxioValue()
	if err != nil {
		return desiredNum, err
	}

	desiredNum = uint32(alluxioValue.ReplicaCount)

	found, err := kubeclient.IsConfigMapExist(e.Client, configMapName, namespace)
	if err != nil {
		return
	}
	if found {
		valueFileName, err = kubectl.SaveConfigMapToFile(configMapName, "data", namespace)
		if err != nil {
			return
		}
	} else {
		// 2.generate value file
		valueFile, err := ioutil.TempFile(os.TempDir(), fmt.Sprintf("%s-%s-values.yaml", name, e.Type()))
		if err != nil {
			e.Log.Error(err, "failed to create tmp file", "valueFile", valueFile.Name())
			return desiredNum, err
		}

		valueFileName = valueFile.Name()
		e.Log.V(1).Info("Save the values file", "valueFile", valueFileName)

		// 2. dump the object into the template file
		err = utils.ToYaml(alluxioValue, valueFile)
		if err != nil {
			return desiredNum, err
		}

		err = kubectl.CreateConfigMapFromFile(configMapName, "data", valueFile.Name(), namespace)
		if err != nil {
			return desiredNum, err
		}
	}

	found, err = helm.CheckRelease(name, namespace)
	if err != nil {
		return desiredNum, err
	}

	if found {
		return desiredNum, nil
	}

	err = helm.InstallRelease(name, namespace, valueFileName, chartName)

	return desiredNum, err
}

// set maxlabel
func (e *AlluxioEngine) SetRuntimeMaxMemory(nodeName string, max string) (err error) {
	var (
		labelNameToAddForAlluxio string = common.LabelAnnotationStorageCapacityPrefix + "human-" + e.Type() + "-" + e.Config.Name
		workerName               string = e.Config.Name + "-" + e.Type() + "-worker"
		namespace                string = common.ALLUXIO_NAMESPACE
	)
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		workers, err := e.getDaemonset(workerName, namespace)
		toUpdate := workers.DeepCopy()
		if err != nil {
			return err
		}

		if _, found := workers.Spec.Template.Labels[labelNameToAddForAlluxio]; !found {
			// Change 143.7GiB to 143.7GB for alluxio
			max = strings.ReplaceAll(max, "i", "")
			toUpdate.Spec.Template.Labels[labelNameToAddForAlluxio] = max
		} else {
			e.Log.Info("The max memory has been set.", labelNameToAddForAlluxio, max)
			return nil
		}

		err = e.Client.Update(context.TODO(), toUpdate)
		if err != nil {
			e.Log.Error(err, "LabelCachedNodes")
		}
		return err
	})

	if err != nil {
		e.Log.Error(err, "SetRuntimeMaxMemory")
	}

	return
}
