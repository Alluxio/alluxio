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
	"k8s.io/apimachinery/pkg/types"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	// Check if the deployment already exists, if not create a new one
	found := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: prestoCluster.Name, Namespace: prestoCluster.Namespace}, found)
	if err != nil {
		if errors.IsNotFound(err) {
			// Define a new deployment
			dep := r.deploymentForPrestoCluster(prestoCluster)
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

// deploymentForMemcached returns a memcached Deployment object
func (r *PrestoClusterReconciler) deploymentForPrestoCluster(m *alluxiocomv1alpha1.PrestoCluster) *appsv1.Deployment {
	labels := map[string]string{"app": "presto", "presto_cr": m.Name}
	replicas := m.Spec.WorkerNum

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
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
					}},
				},
			},
		},
	}
	// Set Memcached instance as the owner and controller
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
