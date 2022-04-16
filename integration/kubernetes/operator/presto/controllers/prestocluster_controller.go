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
	"context"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	alluxiocomv1alpha1 "github.com/Alluxio/alluxio/integration/kubernetes/operator/presto/api/v1alpha1"
)

// PrestoClusterReconciler reconciles a PrestoCluster object
type PrestoClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=alluxio.com,resources=prestoclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=alluxio.com,resources=prestoclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=alluxio.com,resources=prestoclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PrestoCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.11.0/pkg/reconcile
func (r *PrestoClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	prestoCluster := &alluxiocomv1alpha1.PrestoCluster{}
	err := r.Get(ctx, req.NamespacedName, prestoCluster)
	if err != nil {
		logger.Error(err, "Failed to get Presto Cluster")
	}

	isConfigChanged, err := r.ensureLatestConfigMap(ctx, prestoCluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if isConfigChanged {
		logger.Info("Config changed")
	}

	// Check if the deployment already exists, if not create a new one
	found := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: prestoCluster.Name + "-coordinator", Namespace: prestoCluster.Namespace}, found)
	if err != nil {
		if errors.IsNotFound(err) {
			// Define a new deployment
			dep := r.deploymentForPrestoCoordinator(prestoCluster)
			logger.Info("Creating a new Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			err = r.Create(ctx, dep)
			if err != nil {
				logger.Error(err, "Failed to create new Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
				return ctrl.Result{}, err
			}
			// Deployment created successfully - return and requeue
			return ctrl.Result{Requeue: true}, nil
		} else {
			logger.Error(err, "Failed to get Deployment")
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{Requeue: true}, nil
}

func newConfigMap(cr *alluxiocomv1alpha1.PrestoCluster) *corev1.ConfigMap {
	labels := map[string]string{
		"app": cr.Name,
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-coordinator-config",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"jvm.config":        "-server\n-Xmx2G\n-XX:+UseG1GC",
			"config.properties": "node.id=ffffffff-ffff-ffff-ffff-ffffffffffff\nnode.environment=test\nhttp-server.http.port=8080\n\ndiscovery-server.enabled=true\ndiscovery.uri=http://localhost:8080",
		},
	}
}

func (r *PrestoClusterReconciler) ensureLatestConfigMap(ctx context.Context, instance *alluxiocomv1alpha1.PrestoCluster) (bool, error) {
	configMap := newConfigMap(instance)

	// Set presto instance as the owner and controller
	if err := controllerutil.SetControllerReference(instance, configMap, r.Scheme); err != nil {
		return false, err
	}

	// Check if this ConfigMap already exists
	foundMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: configMap.Name, Namespace: configMap.Namespace}, foundMap)
	if err != nil && errors.IsNotFound(err) {
		err = r.Create(ctx, configMap)
		if err != nil {
			return false, err
		}
	} else if err != nil {
		return false, err
	}

	if reflect.DeepEqual(foundMap, configMap) {
		err = r.Update(ctx, configMap)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// deploymentForMemcached returns a memcached Deployment object
func (r *PrestoClusterReconciler) deploymentForPrestoCoordinator(m *alluxiocomv1alpha1.PrestoCluster) *appsv1.Deployment {
	labels := map[string]string{"app": "presto", "presto_cr": m.Name}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name + "-coordinator",
			Namespace: m.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Image: "beinan6666/prestodb",
						Name:  "presto",
						Ports: []corev1.ContainerPort{{
							ContainerPort: 8080,
							Name:          "presto",
						}},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      m.Name + "-coordinator-config-volume",
								ReadOnly:  true,
								MountPath: "/opt/presto/etc",
							},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: m.Name + "-coordinator-config-volume",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: m.Name + "-coordinator-config",
								},
							},
						},
					}},
				},
			},
		},
	}
	// Set presto instance as the owner and controller
	ctrl.SetControllerReference(m, dep, r.Scheme)
	return dep
}

// SetupWithManager sets up the controller with the Manager.
func (r *PrestoClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&alluxiocomv1alpha1.PrestoCluster{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}
