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
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"

	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/utils"
	admissionv1beta1 "k8s.io/api/admission/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// +kubebuilder:webhook:path=/mutate-alluxio-pod,mutating=true,failurePolicy=fail,groups="",resources=pods,verbs=create;update,versions=v1,name=mpod.kb.io

// PodCreateHandler handles Pod
type PodCreateHandler struct {
	// To use the client, you need to do the following:
	// - uncomment it
	// - import sigs.k8s.io/controller-runtime/pkg/client
	// - uncomment the InjectClient method at the bottom of this file.
	Client client.Client

	// Decoder decodes objects
	decoder *admission.Decoder

	Log logr.Logger
}

func (h *PodCreateHandler) mutatingPodFn(ctx context.Context, obj *corev1.Pod) (mutated bool, err error) {
	return h.mutatingPod(ctx, obj)
}

func (h *PodCreateHandler) mutatingPod(ctx context.Context, pod *corev1.Pod) (mutated bool, err error) {

	if !h.mutationRequired(ignoredNamespaces, &pod.ObjectMeta) {
		h.Log.V(1).Info("Skip the pod because it doesn't have annotation data.alluxio.io/dataset",
			"name", pod.Name,
			"namespace", pod.Namespace)
		return
	}

	h.Log.V(1).Info("[pod inject] before mutating", "pod", utils.DumpJSON(pod))

	// pod.Annotations["example-mutating-admission-webhook"] = getDatasetNameFromPod(pod)

	err = h.patchPod(pod)
	if err != nil {
		return
	}

	mutated = true
	h.Log.V(1).Info("Mutate the pod because it doesn't have annotation data.alluxio.io/dataset",
		"name", pod.Name,
		"namespace", pod.Namespace)

	h.Log.Info("[pod inject] after mutating", "pod", utils.DumpJSON(pod))

	return mutated, err
}

func getDatasetNameFromPod(pod *corev1.Pod) (datasetName string) {
	if len(pod.ObjectMeta.Annotations) > 0 {
		datasetName, _ = pod.Annotations[common.LabelAnnotationDataset]
	}
	return datasetName
}

// var _ admission.Handler = &PodCreateHandler{}

// Handle handles admission requests.
func (h *PodCreateHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	obj := &corev1.Pod{}

	err := h.decoder.Decode(req, obj)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	copy := obj.DeepCopy()

	mutated, err := h.mutatingPodFn(ctx, copy)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	if !mutated {
		return admission.Response{
			AdmissionResponse: admissionv1beta1.AdmissionResponse{
				Allowed: true,
			},
		}
	}

	marshaledPod, err := json.Marshal(copy)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

// var _ inject.Client = &PodCreateHandler{}

// InjectClient injects the client into the PodCreateHandler
func (h *PodCreateHandler) InjectClient(c client.Client) error {
	h.Client = c
	return nil
}

// var _ inject.Decoder = &PodCreateHandler{}

// InjectDecoder injects the decoder into the PodCreateHandler
func (h *PodCreateHandler) InjectDecoder(d *admission.Decoder) error {
	h.decoder = d
	return nil
}
