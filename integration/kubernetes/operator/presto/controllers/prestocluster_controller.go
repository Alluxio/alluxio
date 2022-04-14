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
	_ = log.FromContext(ctx)
	prestoCluster := &alluxiocomv1alpha1.PrestoCluster{}
	_ = r.Get(ctx, req.NamespacedName, prestoCluster)
	return ctrl.Result{Requeue: true}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PrestoClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&alluxiocomv1alpha1.PrestoCluster{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}
