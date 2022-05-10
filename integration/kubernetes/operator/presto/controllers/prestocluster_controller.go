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
	"fmt"
	alluxiocomv1alpha1 "github.com/Alluxio/alluxio/integration/kubernetes/operator/presto/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"time"
)

// PrestoClusterReconciler reconciles a PrestoCluster object
type PrestoClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	ConfigHashAnnotationName        = "config_hash"
	AllZeroHash              uint32 = 0
)

//+kubebuilder:rbac:groups=alluxio.com,resources=prestoclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=alluxio.com,resources=prestoclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=alluxio.com,resources=prestoclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *PrestoClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	prestoCluster := &alluxiocomv1alpha1.PrestoCluster{}
	err := r.Get(ctx, req.NamespacedName, prestoCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			logger.Info("Presto cluster's resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Presto Cluster")
	}

	latestCoordinatorConfigHash, err := r.ensureLatestCoordinatorConfigMap(ctx, prestoCluster)
	logger.Info("Checking coordiantor config", "latestCoordinatorConfigHash", latestCoordinatorConfigHash)
	if err != nil {
		logger.Error(err, "Failed to get ConfigMap of the coordinator")
		return ctrl.Result{}, err
	}

	coordinatorDeployment, err := r.getDeployment(ctx, prestoCluster.Name+"-coordinator", prestoCluster.Namespace)
	if err != nil {
		logger.Error(err, "Failed to get Deployment of the coordinator")
		return ctrl.Result{}, err
	}

	if coordinatorDeployment == nil {
		dep := r.deploymentForPrestoCoordinator(prestoCluster, latestCoordinatorConfigHash)
		logger.Info("Creating a new Deployment", "Deployment.Namespace", dep.Namespace,
			"Deployment.Name", dep.Name, "Config version", latestCoordinatorConfigHash)
		err = r.Create(ctx, dep)
		if err != nil {
			logger.Error(err, "Failed to create new Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			return ctrl.Result{}, err
		}
		// Deployment created successfully - return and requeue
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	} else if fmt.Sprint(latestCoordinatorConfigHash) != coordinatorDeployment.Annotations[ConfigHashAnnotationName] {
		logger.Info("The config of coordinator changed, deleting the old coordinator",
			"new config version", latestCoordinatorConfigHash,
			"old config version", coordinatorDeployment.Annotations[ConfigHashAnnotationName])
		err := r.Delete(ctx, coordinatorDeployment)
		if err != nil {
			return ctrl.Result{}, err
		}
		// Deployment deleted successfully - return and requeue
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	latestWorkerConfigHash, err := r.ensureLatestWorkerConfigMap(ctx, prestoCluster)
	logger.Info("Checking worker config", "latestWorkerConfigHash", latestWorkerConfigHash)
	if err != nil {
		logger.Error(err, "Failed to get ConfigMap of the worker")
		return ctrl.Result{}, err
	}

	workerDeployment, err := r.getDeployment(ctx, prestoCluster.Name+"-worker", prestoCluster.Namespace)
	if err != nil {
		logger.Error(err, "Failed to get Deployment of the worker")
		return ctrl.Result{}, err
	}

	if workerDeployment == nil {
		dep := r.deploymentForPrestoWorker(prestoCluster, latestWorkerConfigHash)
		logger.Info("Creating a new Deployment", "Deployment.Namespace", dep.Namespace,
			"Deployment.Name", dep.Name, "Config version", latestWorkerConfigHash)
		err = r.Create(ctx, dep)
		if err != nil {
			logger.Error(err, "Failed to create new Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			return ctrl.Result{}, err
		}
		// Deployment created successfully - return and requeue
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	} else if fmt.Sprint(latestWorkerConfigHash) != workerDeployment.Annotations[ConfigHashAnnotationName] {
		logger.Info("The config of workers changed, deleting the old workers",
			"new config version", latestWorkerConfigHash,
			"old config version", workerDeployment.Annotations[ConfigHashAnnotationName])
		err := r.Delete(ctx, workerDeployment)
		if err != nil {
			return ctrl.Result{}, err
		}
		// Deployment deleted successfully - return and requeue
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	} else if *workerDeployment.Spec.Replicas != prestoCluster.Spec.WorkerSpec.Count {
		workerDeployment.Spec.Replicas = &prestoCluster.Spec.WorkerSpec.Count
		err = r.Update(ctx, workerDeployment)
		if err != nil {
			logger.Error(err, "Failed to update Deployment", "Deployment.Namespace", workerDeployment.Namespace, "Deployment.Name", workerDeployment.Name)
			return ctrl.Result{}, err
		}
		// Requeue after 1 minute to give enough time for the pods be created on the cluster side
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	err = r.createService(ctx, prestoCluster)
	if err != nil {
		logger.Error(err, "Failed to create service for presto coordinator")
	}

	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(prestoCluster.Namespace),
		client.MatchingLabels(map[string]string{"app": "presto", "presto_cr": prestoCluster.Name}),
	}
	if err = r.List(ctx, podList, listOpts...); err != nil {
		logger.Error(err, "Failed to list pods", "Memcached.Namespace", prestoCluster.Namespace, "Memcached.Name", prestoCluster.Name)
		return ctrl.Result{}, err
	}
	for _, pod := range podList.Items {
		prestoCluster.Status
		pod.Status.
	}
	return ctrl.Result{}, nil
}

func (r *PrestoClusterReconciler) ensureLatestCoordinatorConfigMap(ctx context.Context, instance *alluxiocomv1alpha1.PrestoCluster) (uint32, error) {
	configMap := newCoordinatorConfigMap(instance)

	// Set presto instance as the owner and controller
	if err := controllerutil.SetControllerReference(instance, configMap, r.Scheme); err != nil {
		return AllZeroHash, err
	}

	// Check if this ConfigMap already exists
	existingMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: configMap.Name, Namespace: configMap.Namespace}, existingMap)
	if err != nil && errors.IsNotFound(err) {
		err = r.Create(ctx, configMap)
		if err != nil {
			return AllZeroHash, err
		}
		return ConfigDataHash(configMap.Data), nil
	} else if err != nil {
		return AllZeroHash, err
	}

	if !reflect.DeepEqual(existingMap.Data, configMap.Data) {
		err = r.Update(ctx, configMap)
		if err != nil {
			return AllZeroHash, err
		}
		return ConfigDataHash(configMap.Data), nil
	}
	return ConfigDataHash(existingMap.Data), nil
}

func (r *PrestoClusterReconciler) ensureLatestWorkerConfigMap(ctx context.Context, instance *alluxiocomv1alpha1.PrestoCluster) (uint32, error) {
	configMap := newWorkerConfigMap(instance)

	// Set presto instance as the owner and controller
	if err := controllerutil.SetControllerReference(instance, configMap, r.Scheme); err != nil {
		return AllZeroHash, err
	}

	// Check if this ConfigMap already exists
	existingMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: configMap.Name, Namespace: configMap.Namespace}, existingMap)
	if err != nil && errors.IsNotFound(err) {
		err = r.Create(ctx, configMap)
		if err != nil {
			return AllZeroHash, err
		}
		return ConfigDataHash(configMap.Data), nil
	} else if err != nil {
		return AllZeroHash, err
	}

	if !reflect.DeepEqual(existingMap.Data, configMap.Data) {
		err = r.Update(ctx, configMap)
		if err != nil {
			return AllZeroHash, err
		}
		return ConfigDataHash(configMap.Data), nil
	}
	return ConfigDataHash(existingMap.Data), nil
}

func (r *PrestoClusterReconciler) createService(ctx context.Context, m *alluxiocomv1alpha1.PrestoCluster) error {
	serviceLables := map[string]string{"app": "presto", "presto_cr": m.Name, "role": "coordinator_service"}
	serviceName := m.Name + "-coordinator-service"
	services := &corev1.ServiceList{}
	err := r.List(ctx,
		services,
		&client.ListOptions{
			Namespace:     m.Namespace,
			LabelSelector: labels.SelectorFromSet(serviceLables),
		})
	if err != nil {
		return err
	}
	if len(services.Items) > 0 {
		return nil
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: m.Namespace,
			Labels:    serviceLables,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: "http",
					Port: m.Spec.CoordinatorSpec.HttpPort,
				},
			},
			Selector: map[string]string{"app": "presto", "presto_cr": m.Name, "role": "coordinator"},
		},
	}
	return r.Create(ctx, service)
}

func (r *PrestoClusterReconciler) getDeployment(ctx context.Context, name string, namespace string) (*appsv1.Deployment, error) {
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, deployment)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return deployment, nil
}

func (r *PrestoClusterReconciler) deploymentForPrestoCoordinator(m *alluxiocomv1alpha1.PrestoCluster, latestConfigVersion uint32) *appsv1.Deployment {
	labels := map[string]string{"app": "presto", "presto_cr": m.Name, "role": "coordinator"}
	name := m.Name + "-coordinator"
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   m.Namespace,
			Annotations: map[string]string{ConfigHashAnnotationName: fmt.Sprint(latestConfigVersion)},
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
						Image: m.Spec.Image,
						Name:  name,
						Ports: []corev1.ContainerPort{{
							ContainerPort: m.Spec.CoordinatorSpec.HttpPort,
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
								Items: createConfigMountItems(m),
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

func (r *PrestoClusterReconciler) deploymentForPrestoWorker(m *alluxiocomv1alpha1.PrestoCluster, latestConfigVersion uint32) *appsv1.Deployment {
	labels := map[string]string{"app": "presto", "presto_cr": m.Name, "role": "worker"}
	name := m.Name + "-worker"
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   m.Namespace,
			Annotations: map[string]string{ConfigHashAnnotationName: fmt.Sprint(latestConfigVersion)},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &m.Spec.WorkerSpec.Count,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Image: m.Spec.Image,
						Name:  name,
						Ports: []corev1.ContainerPort{{
							ContainerPort: m.Spec.WorkerSpec.HttpPort,
							Name:          "presto",
						}},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      m.Name + "-worker-config-volume",
								ReadOnly:  true,
								MountPath: "/opt/presto/etc",
							},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: m.Name + "-worker-config-volume",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: m.Name + "-worker-config",
								},
								Items: createConfigMountItems(m),
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
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}
