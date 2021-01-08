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
	"os"
	"reflect"

	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/utils"
	"github.com/Alluxio/alluxio/pkg/utils/kubeclient"
	units "github.com/docker/go-units"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	data "github.com/Alluxio/alluxio/api/v1alpha1"
)

// Load the data
func (e *AlluxioEngine) LoadData() error {
	var (
		namespace string = common.ALLUXIO_NAMESPACE
		name      string = e.Config.Name + "-" + e.Type() + "-loader"
	)

	requireLoadData := false
	dataset, err := utils.GetDataset(e.Client, e.Config.Name, e.Config.Namespace)
	if err != nil {
		return err
	}

	runtime, err := utils.GetRuntime(e.Client, e.Config.Name, e.Config.Namespace)
	if err != nil {
		return err
	}
	// 1. Check if the pod who is responsible for loading data
	pod, err := kubeclient.GetPodByName(e.Client, name, namespace)
	if err != nil {
		return err
	}

	if pod != nil {
		// If the loading data is active, then exit
		if !kubeclient.IsCompletePod(pod) {
			e.Log.V(1).Info("Loading data is in progress, skip")
			return nil
		}

		// If the pod is failed, then let the user to check why it's failed, and delete it manually
		if kubeclient.IsFailedPod(pod) {
			info := fmt.Sprintf("Please check the log of the pod %s with namespace %s",
				name, namespace)
			e.Log.Info("The loading data is failed, please check the log of the pod",
				"name", name, "namespace", namespace)
			cond := utils.NewDatasetCachedCondition(data.Preloaded, info,
				info,
				corev1.ConditionFalse)
			if !utils.IsDatasetCachedConditionExist(dataset.Status.CacheStatus.Conditions, cond) {
				err = e.updateDatasetCondition(cond)
			}
			return err
		} else {
			_, err = e.UpdateRuntimeStatus(runtime)
			if err != nil {
				e.Log.Error(err, "Failed to updatet runtime", "name", e.Config.Name)
				return err
			}

			datasetToUpdate := dataset.DeepCopy()
			datasetToUpdate.Status.CacheStatus.CacheStates = runtime.Status.CacheStates
			// datasetToUpdate.Status.CacheStatus.CacheStates =

			if !reflect.DeepEqual(dataset.Status, datasetToUpdate.Status) {
				err = e.Client.Status().Update(context.TODO(), datasetToUpdate)
				if err != nil {
					e.Log.Error(err, "Update dataset")
					return err
				}
				dataset = datasetToUpdate
			}

			info := "The dataset has been loaded completely"
			cond := utils.NewDatasetCachedCondition(data.Preloaded, info,
				info,
				corev1.ConditionTrue)

			if !utils.IsDatasetCachedConditionExist(dataset.Status.CacheStatus.Conditions, cond) {
				err = e.updateDatasetCondition(cond)
				if err != nil {
					return err
				}
			}

		}
	}

	// 2. Check if the action of loading data is required
	cachedStr := "0"
	if dataset.Status.CacheStatus.CacheStates[data.Cached] != "" {
		cachedStr = dataset.Status.CacheStatus.CacheStates[data.Cached]
	}

	cached, err := units.RAMInBytes(cachedStr)
	if err != nil {
		return err
	}

	lowmarkStr := "0"
	if dataset.Status.CacheStatus.CacheStates[data.LowWaterMark] != "" {
		lowmarkStr = dataset.Status.CacheStatus.CacheStates[data.LowWaterMark]
	}

	lowMark, err := units.RAMInBytes(lowmarkStr)
	if err != nil {
		return err
	}
	var (
		percentage float64 = 0
		unit       string  = ""
	)
	percentStr := "0.00 %"
	if dataset.Status.CacheStatus.CacheStates[data.CachedPercentage] != "" {
		percentStr = dataset.Status.CacheStatus.CacheStates[data.CachedPercentage]
	}

	_, err = fmt.Sscanf(percentStr, "%f%s", &percentage, &unit)
	if err != nil {
		return err
	}

	e.Log.V(1).Info("percentage unit ", "percentage", percentage,
		"unit", unit)

	fileSize, inAllxuio, err := e.du()
	if err != nil {
		return err
	}

	if float64(cached) < float64(lowMark) && percentage < 50.0 && fileSize == inAllxuio {
		e.Log.Info("Need load the data because percent is lower than expected",
			"cached", cachedStr,
			"lowMark", lowmarkStr,
			"percentage", percentage,
			"fileSize", fileSize,
			"inAlluxio", inAllxuio)
		requireLoadData = true
	} else {
		e.Log.Info("Skip loading the data", "cached", cachedStr,
			"lowMark", lowmarkStr,
			"percentage", percentage,
			"fileSize", fileSize,
			"inAlluxio", inAllxuio)
	}

	// 3. launch the pod who is responsible for loading the data
	if requireLoadData {
		if pod != nil {
			err := kubeclient.DeletePod(e.Client, pod)
			if err != nil {
				return err
			}
		}

		replicas := *dataset.Spec.Replicas
		workers := int32(runtime.Status.WorkerNumberReady)
		if replicas > workers {
			replicas = workers
		}

		err = e.launchExecutorForLoadingData(name, namespace, replicas)
		if err != nil {
			return err
		}
		info := "The dataset is loading"
		cond := utils.NewDatasetCachedCondition(data.Preloading, info,
			info,
			corev1.ConditionTrue)
		err = e.updateDatasetCondition(cond)
		if err != nil {
			return err
		}

	}

	return nil

}

func (e *AlluxioEngine) launchExecutorForLoadingData(name, namespace string, replicas int32) (err error) {

	var (
		masterName string = e.Config.Name + "-" + e.Type() + "-master"
	)

	master, err := e.getMasterStatefulset(masterName, namespace)
	if err != nil {
		return err
	}

	container := &corev1.Container{
		Name: "launcher",
		// Env:
		Command: []string{"time",
			"/opt/alluxio/bin/alluxio",
			"fs",
			"distributedLoad",
			fmt.Sprintf("-Dalluxio.job.master.hostname=%s", masterName),
			fmt.Sprintf("-Dalluxio.master.hostname=%s", masterName),
			"--replication",
			fmt.Sprintf("%d", replicas),
			"/"},
	}

	container.Image = os.Getenv(common.ALLUXIO_DATA_LOADER_IMAGE_ENV)
	if container.Image == "" {
		if len(master.Spec.Template.Spec.Containers) > 0 {
			container.Image = master.Spec.Template.Spec.Containers[0].Image
		}

		// container.Image = common.DEFAULT_ALLUXIO_DATA_LOADER_IMAGE
	}

	podExecutor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{},
		},
		Spec: corev1.PodSpec{
			HostNetwork:   true,
			DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{*container},
		},
	}

	createOps := &client.CreateOptions{}

	err = e.Client.Create(context.TODO(), podExecutor, createOps)
	if err != nil {
		e.Log.Error(err, "launchExecutorForLoadingData")
	}

	return err
}

func (e *AlluxioEngine) updateDatasetCondition(cond data.CacheCondition) (err error) {

	// TODO(cheyang): add condition for the user aware of this
	// 5.Setup the init status of Dataset
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		dataset, err := utils.GetDataset(e.Client, e.Config.Name, e.Config.Namespace)
		if err != nil {
			e.Log.Error(err, "Get dataset")
			return err
		}

		datasetToUpdate := dataset.DeepCopy()

		datasetToUpdate.Status.CacheStatus.Conditions = utils.UpdateDatasetCachedCondition(datasetToUpdate.Status.CacheStatus.Conditions,
			cond)

		if !reflect.DeepEqual(dataset.Status, datasetToUpdate.Status) {
			err = e.Client.Status().Update(context.TODO(), datasetToUpdate)
			if err != nil {
				e.Log.Error(err, "Update dataset")
				return err
			}
		}

		return nil

		// return b.UpdateCacheStateOfDataset(datasetToUpdate)

	})

	return err

}
