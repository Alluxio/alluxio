/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	alluxiocomv1alpha1 "github.com/Alluxio/alluxio/integration/kubernetes/operator/presto/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func createConfigMountItems(cr *alluxiocomv1alpha1.PrestoCluster) []corev1.KeyToPath {
	mountItems := []corev1.KeyToPath{
		{
			Key:  "config.properties",
			Path: "config.properties",
		},
		{
			Key:  "jvm.config",
			Path: "jvm.config",
		},
	}
	for _, catalog := range cr.Spec.Catalogs {
		mountItems = append(mountItems, corev1.KeyToPath{
			Key:  catalog.Name,
			Path: "catalog/" + catalog.Name + ".properties",
		})
	}
	return mountItems
}
