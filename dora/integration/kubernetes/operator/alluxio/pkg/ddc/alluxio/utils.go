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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/utils/kubeclient"
	"github.com/Alluxio/alluxio/pkg/utils/kubectl"
	units "github.com/docker/go-units"
	yaml "gopkg.in/yaml.v2"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	options "sigs.k8s.io/controller-runtime/pkg/client"

	"io/ioutil"
	"os"
)

func (e *AlluxioEngine) getMasterStatefulset(name string, namespace string) (master *appsv1.StatefulSet, err error) {
	master = &appsv1.StatefulSet{}
	err = e.Client.Get(context.TODO(), types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, master)

	return master, err
}

func (e *AlluxioEngine) getDaemonset(name string, namespace string) (daemonset *appsv1.DaemonSet, err error) {
	daemonset = &appsv1.DaemonSet{}
	err = e.Client.Get(context.TODO(), types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, daemonset)

	return daemonset, err
}

func (e *AlluxioEngine) getConfigMap(name string, namespace string) (configMap *corev1.ConfigMap, err error) {
	configMap = &corev1.ConfigMap{}
	err = e.Client.Get(context.TODO(), types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, configMap)

	return configMap, err
}

/**
* create the template file from configmap, it may lack of the node selector
 */
func (e *AlluxioEngine) getOrCreateTemplateValueFile() (fileName string, err error) {
	// 1. Found if the specified configmap with name in alluxio-system namespace
	var (
		name      string = e.Config.Name + "-" + e.Type() + "-template"
		namespace string = common.ALLUXIO_NAMESPACE
	)

	_, err = e.getConfigMap(name, namespace)
	// You can get the value file directly
	if err == nil {
		fileName, err = kubectl.SaveConfigMapToFile(name, "data", namespace)
		if err != nil {
			return fileName, err
		}
		// return the file directly
		return
	} else if !apierrs.IsNotFound(err) {
		return
	}

	// 2. If the specified template configmap doesn't exist, get and generate the default configmap

	name = "default-" + e.Type() + "-template"
	_, err = e.getConfigMap(name, namespace)
	if err == nil {
		fileName, err = kubectl.SaveConfigMapToFile(name, "data", namespace)
		if err != nil {
			return fileName, err
		}
		// return the file directly
		return
	} else if !apierrs.IsNotFound(err) {
		return
	}

	// 3. use the default configmap to generate specifed configmap
	tmpFile, err := ioutil.TempFile(os.TempDir(), "default")
	if err != nil {
		e.Log.Error(err, "generate the default config", "file", tmpFile.Name())
		return
	}

	err = ioutil.WriteFile(tmpFile.Name(), []byte(defaultConfig), permissions)
	if err != nil {
		return
	}

	err = kubectl.CreateConfigMapFromFile(name, "data", tmpFile.Name(), namespace)
	return tmpFile.Name(), err
}

// generate alluxio struct
func (e *AlluxioEngine) generateAlluxioValue() (value *Alluxio, err error) {

	labelName := common.LabelAnnotationStorageCapacityPrefix + e.Type() + "-" + e.Config.Name
	//1. Get the template value file
	templateFileName, err := e.getOrCreateTemplateValueFile()
	if err != nil {
		return
	}

	//2. Generate the new value file according to template
	value = &Alluxio{}
	data, err := ioutil.ReadFile(templateFileName)
	if err != nil {
		return
	}

	err = yaml.Unmarshal(data, value)
	if err != nil {
		return
	}

	// // Set the node selector
	// value.NodeSelector = map[string]string{
	// 	labelName: "true",
	// }

	// Set the node selector for worker
	if len(value.Woker.NodeSelector) == 0 {
		value.Woker.NodeSelector = map[string]string{}
	}
	value.Woker.NodeSelector[labelName] = "true"
	if len(value.Fuse.NodeSelector) == 0 {
		value.Fuse.NodeSelector = map[string]string{}
	}
	value.Fuse.NodeSelector[labelName] = "true"

	// Set the option to access dataset
	// for _, mount := range e.Config.DatasetConfig.Mounts {
	// 	key := fmt.Sprintf("alluxio.master.mount.table.%s.ufs", mount.Name)
	// 	value.Properties[key] = mount.MountPoint
	// 	key = fmt.Sprintf("alluxio.master.mount.table.%s.alluxio", mount.Name)
	// 	value.Properties[key] = fmt.Sprintf("/%s", mount.Name)
	// 	key = fmt.Sprintf("alluxio.master.mount.table.%s.readonly", mount.Name)
	// 	value.Properties[key] = strconv.FormatBool(mount.ReadOnly)
	// 	for k, v := range mount.Options {
	// 		value.Properties[k] = v
	// 	}
	// }

	// Set water mark
	levels := []Level{}
	for _, level := range value.Tieredstore.Levels {
		level.Low = e.Config.LowWaterMarkRatio
		level.High = e.Config.HighWaterMarkRatio
		level.Path = common.CacheStoragePath
		levels = append(levels, level)
	}
	value.Tieredstore.Levels = levels

	// set the replicas as 1
	if value.ReplicaCount == 0 {
		value.ReplicaCount = 1
	}

	return
}

// getCurrentCacheCapacityOfNode cacluates the node
func (e *AlluxioEngine) getCurrentCacheCapacityOfNode(nodeName string) (capacity int64, err error) {
	var (
		labelNameToAddRaw = common.LabelAnnotationStorageCapacityPrefix + "raw-" + e.Type() + "-" + e.Config.Name
		// labelNameToAddKiB = common.LabelAnnotationStorageCapacityPrefix + "KB-" + b.Type() + "-" + b.Config.Name
		// labelNameToAddMiB = common.LabelAnnotationStorageCapacityPrefix + "MB-" + b.Type() + "-" + b.Config.Name
		// labelNameToAddGiB = common.LabelAnnotationStorageCapacityPrefix + "GB-" + b.Type() + "-" + b.Config.Name
		labelNameToAdd = common.LabelAnnotationStorageCapacityPrefix + "human-" + e.Type() + "-" + e.Config.Name
		// labelName      = common.LabelAnnotationStorageCapacityPrefix + e.Type() + "-" + e.Config.Name
	)

	node, err := kubeclient.GetNode(e.Client, nodeName)
	if err != nil {
		return capacity, err
	}

	if !kubeclient.IsReady(*node) {
		e.Log.V(1).Info("Skip the not ready node", "node", capacity)
		return 0, nil
	}

	for k, v := range node.Labels {
		if k == labelNameToAdd {
			value := "0"
			if v != "" {
				value = v
			}
			// capacity = units.BytesSize(float64(value))
			capacity, err = units.RAMInBytes(value)
			if err != nil {
				return capacity, err
			}
			e.Log.V(1).Info("getCurrentCacheCapacityOfNode", k, value)
			e.Log.V(1).Info("getCurrentCacheCapacityOfNode byteSize", k, capacity)
		}

		if k == labelNameToAddRaw {
			e.Log.V(1).Info("getCurrentCacheCapacityOfNode raw byteSize", k, v)
		}
	}

	return
}

// getRunningPodsOfDaemonset gets worker pods
func (e *AlluxioEngine) getRunningPodsOfDaemonset(dsName, namespace string) (pods []corev1.Pod, err error) {

	ds, err := e.getDaemonset(dsName, namespace)
	if err != nil {
		return pods, err
	}

	selector := ds.Spec.Selector.MatchLabels
	// labels := selector.MatchLabels

	pods = []corev1.Pod{}
	podList := &corev1.PodList{}
	err = e.Client.List(context.TODO(), podList, options.InNamespace(namespace), options.MatchingLabels(selector))
	if err != nil {
		return pods, err
	}

	for _, pod := range podList.Items {
		if !podutil.IsPodReady(&pod) {
			e.Log.V(1).Info("Skip the pod because it's not ready", "pod", pod.Name, "namespace", pod.Namespace)
			continue
		}
		pods = append(pods, pod)
	}

	return pods, nil

}

// getCachedCapacityOfNode cacluates the node
func (e *AlluxioEngine) GetCurrentCachedCapacity() (totalCapacity uint64, workerNum uint32, err error) {
	var (
		namespace  string = common.ALLUXIO_NAMESPACE
		workerName string = e.Config.Name + "-" + e.Type() + "-worker"
	)

	pods, err := e.getRunningPodsOfDaemonset(workerName, namespace)
	if err != nil {
		return totalCapacity, workerNum, err
	}
	nodeMap := map[string]bool{}

	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if nodeName == "" {
			e.Log.Info("The node is skipped due to its node name is null", "node", pod.Spec.NodeName,
				"pod", pod.Name, "namespace", namespace)
			continue
		} else {
			_, found := nodeMap[nodeName]
			if found {
				e.Log.Info("The node is skipped due to it's already caculated", "node", pod.Spec.NodeName,
					"pod", pod.Name, "namespace", namespace)
				continue
			}
		}

		capacity, err := e.getCurrentCacheCapacityOfNode(nodeName)
		if err != nil {
			return totalCapacity, workerNum, err
		}
		totalCapacity += uint64(capacity)
		workerNum += 1
	}

	return totalCapacity, workerNum, err
}
