package main

import (
	"github.com/Alluxio/pillars/pkg/common"
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
