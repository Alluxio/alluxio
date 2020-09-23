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
	"github.com/Alluxio/alluxio/pkg/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var ignoredNamespaces = []string{
	metav1.NamespaceSystem,
	metav1.NamespacePublic,
}

// Check whether the target resoured need to be mutated
func (h *PodCreateHandler) mutationRequired(ignoredList []string, metadata *metav1.ObjectMeta) (required bool) {
	// skip special kubernete system namespaces
	for _, namespace := range ignoredList {
		if metadata.Namespace == namespace {
			h.Log.Info("Skip mutation for it's in special namespace:",
				"name", metadata.Name,
				"namespace", metadata.Namespace)

			return false
		}
	}

	annotations := metadata.GetAnnotations()
	if len(annotations) > 0 {
		_, ok := annotations[common.LabelAnnotationDataset]
		if ok {
			required = true
		}
	}

	h.Log.Info("Mutation policy", "name", metadata.Name, "namespace", metadata.Namespace, "required", required)

	return required
}

// // Check whether the target resoured need to be mutated.
// func podNeedsInjection(pod *corev1.Pod) (need bool) {
// 	if len(pod.ObjectMeta.Annotations) > 0 {
// 		_, ok := pod.Annotations[common.LabelAnnotationDataset]
// 		if ok {
// 			need = true
// 		}
// 	}
// 	return need
// }
