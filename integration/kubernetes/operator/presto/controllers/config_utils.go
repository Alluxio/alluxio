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
	"fmt"
	alluxiocomv1alpha1 "github.com/Alluxio/alluxio/integration/kubernetes/operator/presto/api/v1alpha1"
	"hash/fnv"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sort"
	"strings"
)

func newCoordinatorConfigMap(cr *alluxiocomv1alpha1.PrestoCluster) *corev1.ConfigMap {
	labels := map[string]string{
		"app": cr.Name,
	}
	var jvmConfigBuilder strings.Builder
	jvmConfigBuilder.WriteString("-server\n")
	jvmConfigBuilder.WriteString("-XX:+UseG1GC\n")
	jvmConfigBuilder.WriteString(fmt.Sprintf("-Xmx%s\n", "1G"))
	jvmConfigBuilder.WriteString(cr.Spec.CoordinatorSpec.AdditionalJvmOptions)

	var configPropsBuilder strings.Builder
	configPropsBuilder.WriteString(fmt.Sprintf("node.environment=%s\n", cr.Spec.Environment))
	configPropsBuilder.WriteString(fmt.Sprintf("http-server.http.port=%d\n", cr.Spec.CoordinatorSpec.HttpPort))
	configPropsBuilder.WriteString(fmt.Sprintf("discovery-server.enabled=%v\n", true))
	configPropsBuilder.WriteString(fmt.Sprintf("discovery.uri=%v\n", "http://localhost:8080"))

	var keys []string
	for key, _ := range cr.Spec.CoordinatorSpec.AdditionalConfigs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		configPropsBuilder.WriteString(fmt.Sprintf("%s=%s\n", key, cr.Spec.CoordinatorSpec.AdditionalConfigs[key]))
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-coordinator-config",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"jvm.config":        strings.TrimSpace(jvmConfigBuilder.String()),
			"config.properties": strings.TrimSpace(configPropsBuilder.String()),
		},
	}
}

func newWorkerConfigMap(cr *alluxiocomv1alpha1.PrestoCluster) *corev1.ConfigMap {
	labels := map[string]string{
		"app": cr.Name,
	}
	var jvmConfigBuilder strings.Builder
	jvmConfigBuilder.WriteString("-server\n")
	jvmConfigBuilder.WriteString("-XX:+UseG1GC\n")
	jvmConfigBuilder.WriteString(fmt.Sprintf("-Xmx%s\n", "1G"))
	jvmConfigBuilder.WriteString(cr.Spec.WorkerSpec.AdditionalJvmOptions)

	var configPropsBuilder strings.Builder
	configPropsBuilder.WriteString(fmt.Sprintf("node.environment=%s\n", cr.Spec.Environment))
	configPropsBuilder.WriteString(fmt.Sprintf("http-server.http.port=%d\n", cr.Spec.WorkerSpec.HttpPort))
	configPropsBuilder.WriteString(fmt.Sprintf("discovery.uri=http://%v:%d\n", cr.Name+"-coordinator-service", cr.Spec.WorkerSpec.HttpPort))

	var keys []string
	for key, _ := range cr.Spec.WorkerSpec.AdditionalConfigs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		configPropsBuilder.WriteString(fmt.Sprintf("%s=%s\n", key, cr.Spec.WorkerSpec.AdditionalConfigs[key]))
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-worker-config",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"jvm.config":        strings.TrimSpace(jvmConfigBuilder.String()),
			"config.properties": strings.TrimSpace(configPropsBuilder.String()),
		},
	}
}

func ConfigDataHash(config map[string]string) uint32 {
	var hashcode uint32 = 0
	for key, value := range config {
		hashcode ^= hashKeyValuePair(key, value)
	}
	return hashcode
}

func hashKeyValuePair(key, value string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(strings.TrimSpace(key)))
	h.Write([]byte(strings.TrimSpace(value)))
	return h.Sum32()
}
