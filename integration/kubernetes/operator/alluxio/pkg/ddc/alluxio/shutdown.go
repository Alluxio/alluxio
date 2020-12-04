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
	"fmt"
	"strings"

	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/utils/helm"
	"github.com/Alluxio/alluxio/pkg/utils/kubeclient"
)

// DestroyMaster Destroies the master
func (e *AlluxioEngine) DestroyMaster() (err error) {
	var (
		namespace = common.ALLUXIO_NAMESPACE
		name      = e.Config.Name
		found     = false
	)

	found, err = helm.CheckRelease(name, namespace)
	if err != nil {
		return err
	}

	if found {
		err = helm.DeleteRelease(name, namespace)
		if err != nil {
			return
		}
	}
	return
}

// // Destroy the workers
// func (e *AlluxioEngine) DestroyWorkers() error {
// 	return nil
// }

// CleanupCache cleans up the cache
func (e *AlluxioEngine) CleanupCache() (err error) {
	// TODO(cheyang): clean up the cache
	err = e.invokeCleanCache("/")
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}
	cached, err := e.cachedState()
	if err != nil {
		return
	}

	if cached > 0 {
		return fmt.Errorf("The remaining cached is not cleaned up, it still has %d", cached)
	}

	return nil
}

// CleanAll cleans up the all
func (e *AlluxioEngine) CleanAll() (err error) {
	var (
		valueConfigmapName    = e.Config.Name + "-" + e.Type() + "-values"
		templateConfigmapName = e.Config.Name + "-" + e.Type() + "-template"
		configName            = e.Config.Name + "-" + e.Type() + "-config"
		formatScriptName      = e.Config.Name + "-" + e.Type() + "-format-script"
		fuseConfigName        = e.Config.Name + "-" + e.Type() + "-fuse-config"
		loader                = e.Config.Name + "-" + e.Type() + "-loader"
		namespace             = common.ALLUXIO_NAMESPACE
	)

	cms := []string{valueConfigmapName, templateConfigmapName, configName, formatScriptName, fuseConfigName}

	for _, cm := range cms {
		err = e.deleteConfigMap(cm, namespace)
		if err != nil {
			return
		}
	}

	pod, err := kubeclient.GetPodByName(e.Client, loader, namespace)
	if err != nil {
		return err
	}

	if pod != nil {
		err = kubeclient.DeletePod(e.Client, pod)
		if err != nil {
			return
		}
	}

	return nil
}

func (e *AlluxioEngine) deleteConfigMap(name string, namespace string) (err error) {
	found, err := kubeclient.IsConfigMapExist(e.Client, name, namespace)
	if found {
		err = kubeclient.DeleteConfigMap(e.Client, name, namespace)
		if err != nil {
			return err
		}
	}
	return nil
}
