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

package kubeclient

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsCompletePod determines if the pod is complete
func IsCompletePod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	if pod.DeletionTimestamp != nil {
		return true
	}

	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return true
	}
	return false
}

// IsFailedPod determines if the pod is failed
func IsFailedPod(pod *corev1.Pod) bool {
	return pod != nil && pod.Status.Phase == corev1.PodFailed
}

func GetPodByName(client client.Client, name, namespace string) (pod *corev1.Pod, err error) {
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	pod = &corev1.Pod{}

	if err = client.Get(context.TODO(), key, pod); err != nil {
		if apierrs.IsNotFound(err) {
			err = nil
			pod = nil
		}
		return pod, err
	}

	return
}

func DeletePod(client client.Client, pod *corev1.Pod) error {
	err := client.Delete(context.TODO(), pod)
	if apierrs.IsNotFound(err) {
		err = nil
	}
	return err
}
