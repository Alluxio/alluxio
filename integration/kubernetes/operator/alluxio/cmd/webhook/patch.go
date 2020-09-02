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

package main

import (
	"fmt"

	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/utils"
	corev1 "k8s.io/api/core/v1"
)

const (
	defaultHostPath      string = "/alluxio-fuse"
	mountCheckName       string = "alluxio-mount-check"
	mountCheckVolumeName string = "alluxio-check"
	mountCheckPath       string = "/target"
)

func (h *PodCreateHandler) patchPod(pod *corev1.Pod) (err error) {
	h.patchNodeSelector(pod)
	h.patchInitContainers(pod)
	return h.patchMountPath(pod)
}

func (h *PodCreateHandler) patchMountPath(pod *corev1.Pod) (err error) {
	// pod.Annotations["example-mutating-admission-webhook"] = getDatasetNameFromPod(pod)

	if len(pod.Spec.Volumes) == 0 {
		pod.Spec.Volumes = []corev1.Volume{}
	}

	name := getDatasetNameFromPod(pod)
	if len(name) < 59 {
		name = fmt.Sprintf("%s-%s", name, utils.StringWithCharset(4))
	} else if len(name) > 64 {
		name = name[:64]
	}

	// 1. Add volumes
	volumeFound := false
	for _, volume := range pod.Spec.Volumes {
		if volume.HostPath != nil {
			if volume.HostPath.Path == defaultHostPath {
				volumeFound = true
			}
		}
	}

	if !volumeFound {
		datasetVolume := corev1.Volume{}
		directoryType := corev1.HostPathDirectory
		datasetVolume.Name = name
		datasetVolume.HostPath = &corev1.HostPathVolumeSource{
			Path: defaultHostPath,
			Type: &directoryType,
		}

		pod.Spec.Volumes = append(pod.Spec.Volumes, datasetVolume)
	}

	// 2. Add Volume mount
	modifiedContainers := []corev1.Container{}
	for _, container := range pod.Spec.Containers {
		if len(container.VolumeMounts) == 0 {
			container.VolumeMounts = []corev1.VolumeMount{}
		}

		mount := corev1.VolumeMount{
			Name: name,
			// MountPath: fmt.Sprintf("/%s", getDatasetNameFromPod(pod)),
			MountPath: "/dataset",
			ReadOnly:  true,
		}

		container.VolumeMounts = append(container.VolumeMounts, mount)
		modifiedContainers = append(modifiedContainers, container)
	}

	pod.Spec.Containers = modifiedContainers

	return nil
}

// patch node selectors
func (h *PodCreateHandler) patchNodeSelector(pod *corev1.Pod) {
	var (
		datasetName     = getDatasetNameFromPod(pod)
		labelCommonName = common.LabelAnnotationStorageCapacityPrefix + datasetName
		// modifedSelectors = map[string]string{}
	)

	modifedSelectors := pod.Spec.NodeSelector
	if len(modifedSelectors) == 0 {
		modifedSelectors = map[string]string{}
	}

	modifedSelectors[labelCommonName] = "true"
	pod.Spec.NodeSelector = modifedSelectors
}

func (h *PodCreateHandler) patchInitContainers(pod *corev1.Pod) {
	modifiedInitContainers := pod.Spec.InitContainers

	if len(modifiedInitContainers) == 0 {
		modifiedInitContainers = []corev1.Container{}
	}

	// 1. create init containers
	bFlag := true
	propagationBidirectional := corev1.MountPropagationBidirectional
	container := corev1.Container{
		Name:            mountCheckName,
		Image:           alluxioInitContainerImage,
		ImagePullPolicy: corev1.PullAlways,
		SecurityContext: &corev1.SecurityContext{Privileged: &bFlag},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:             mountCheckVolumeName,
				MountPath:        mountCheckPath,
				MountPropagation: &propagationBidirectional,
			},
		},
	}

	modifiedInitContainers = append(modifiedInitContainers, container)
	pod.Spec.InitContainers = modifiedInitContainers

	if len(pod.Spec.Volumes) == 0 {
		pod.Spec.Volumes = []corev1.Volume{}
	}

	// 2. create volume
	tmpVolume := corev1.Volume{}
	tmpType := corev1.HostPathDirectory
	tmpVolume.Name = mountCheckVolumeName
	tmpVolume.HostPath = &corev1.HostPathVolumeSource{
		Path: "/proc",
		Type: &tmpType,
	}

	pod.Spec.Volumes = append(pod.Spec.Volumes, tmpVolume)

}
