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

package v1alpha1

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/Alluxio/alluxio/api/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/utils"
)

// DatasetReconciler reconciles a Dataset object
type DatasetReconciler struct {
	client.Client
	Recorder     record.EventRecorder
	Log          logr.Logger
	Scheme       *runtime.Scheme
	ResyncPeriod time.Duration
}

type reconcileRequestContext struct {
	context.Context
	Log     logr.Logger
	Dataset datav1alpha1.Dataset
	types.NamespacedName
}

// +kubebuilder:docs-gen:collapse=Clock

// +kubebuilder:rbac:groups=data.alluxio.io/v1aphla1,resources=datasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=data.alluxio.io/v1aphla1,resources=datasets/status,verbs=get;update;patch
func (r *DatasetReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := reconcileRequestContext{
		Context:        context.Background(),
		Log:            r.Log.WithValues("dataset", req.NamespacedName),
		NamespacedName: req.NamespacedName,
	}

	notFound := false
	ctx.Log.V(1).Info("process the request", "request", req)

	/*
		###1. Load the dataset
	*/
	if err := r.Get(ctx, req.NamespacedName, &ctx.Dataset); err != nil {
		ctx.Log.Info("Unable to fetch Dataset", "reason", err)
		if utils.IgnoreNotFound(err) != nil {
			r.Log.Error(err, "failed to get dataset")
			return ctrl.Result{}, err
		} else {
			notFound = true
		}
	} else {
		return r.reconcileDataset(ctx)
	}

	/*
		### 2.   we'll ignore not-found errors, since they can't be fixed by an immediate
		 requeue (we'll need to wait for a new notification), and we can get them
		 on deleted requests.
	*/
	if notFound {
		ctx.Log.V(1).Info("Not found.")
	}
	return ctrl.Result{}, nil
}

// reconcile Dataset
func (r *DatasetReconciler) reconcileDataset(ctx reconcileRequestContext) (ctrl.Result, error) {
	log := ctx.Log.WithName("reconcileDataset")
	log.V(1).Info("process the dataset", "dataset", ctx.Dataset)
	var err error = nil

	// 1. Check if need to delete dataset
	if utils.HasDeletionTimestamp(ctx.Dataset.ObjectMeta) {
		return r.reconcileDatasetDeletion(ctx)
	}

	// 2.Add finalizer
	if !utils.ContainsString(ctx.Dataset.ObjectMeta.GetFinalizers(), common.AlluxioDatasetResourceFinalizerName) {
		return r.addFinalizerAndRequeue(ctx)
	}

	// 3.Try to create runtime
	var runtime datav1alpha1.Runtime
	if err := r.Get(ctx, ctx.NamespacedName, &runtime); err != nil {
		if utils.IgnoreNotFound(err) == nil {
			log.V(1).Info("Create the ddc runtime", "runtime", ctx.NamespacedName)
			// 3.1 Construct runtime
			runtime, err = constructRuntimeForDataset(ctx.Dataset)
			if err != nil {
				log.Error(err, "Failed to construct runtime")
				return utils.RequeueIfError(err)
			}

			// 3.2 Create the runtime
			if err := r.Create(ctx, &runtime); err != nil {
				log.Error(err, "Failed to create runtime")
				return utils.RequeueIfError(err)
			}
		} else {
			log.Error(err, "Failed to get the ddc runtime when trying to create it.")
			return utils.RequeueIfError(err)
		}
	} else {
		// If the runtime is not set, use the default ddc runtime
		runtimeType := ctx.Dataset.Spec.Runtime
		if runtimeType == "" {
			runtimeType = common.DefaultDDCRuntime
		}
		if runtime.Spec.Type == runtimeType {
			log.V(1).Info("the ddc runtime has been created, then reuse it", "runtime", ctx.NamespacedName)
		}

	}

	if utils.IgnoreNotFound(err) != nil {
		if &ctx.Dataset != nil {
			r.Recorder.Eventf(&ctx.Dataset, corev1.EventTypeWarning, common.ErrorProcessDatasetReason, "Process Dataset error %v", err)
		}
		return utils.RequeueIfError(err)
	}
	// return utils.RequeueAfterInterval(r.ResyncPeriod)
	return utils.NoRequeue()
}

// reconcile Dataset Deletion
func (r *DatasetReconciler) reconcileDatasetDeletion(ctx reconcileRequestContext) (ctrl.Result, error) {
	log := ctx.Log.WithName("reconcileDatasetDeletion")
	log.V(1).Info("process the dataset", "dataset", ctx.Dataset)

	// 1. Destroy runtime
	var runtime datav1alpha1.Runtime
	deleted := false
	if err := r.Get(ctx, ctx.NamespacedName, &runtime); err != nil {
		if utils.IgnoreNotFound(err) == nil {
			log.V(1).Info("The runtime is not found when trying to delete it", "runtime", ctx.NamespacedName)
			deleted = true
		} else {
			log.Error(err, "Failed to get the ddc runtime")
			return utils.RequeueIfError(err)
		}
	} else {
		log.V(1).Info("Try to delete the ddc runtime.", "runtime", &runtime)
		if err := r.Delete(ctx, &runtime); err != nil {
			if utils.IgnoreNotFound(err) == nil {
				log.V(1).Info("The runtime is already deleted when trying to delete it", "runtime", ctx.NamespacedName)
			} else {
				log.Error(err, "Failed to delete the ddc runtime")
				return utils.RequeueIfError(err)
			}
		} else {
			log.V(1).Info("Delete the ddc runtime successfully.", "runtime", &runtime)
		}
	}

	// 2. Check if the runtime is deleted
	if !deleted {
		if err := r.Get(ctx, ctx.NamespacedName, &runtime); err != nil {
			if utils.IgnoreNotFound(err) == nil {
				log.V(1).Info("The runtime is not found when trying to delete it", "runtime", ctx.NamespacedName)
				deleted = true
			} else {
				log.Error(err, "Failed to get the ddc runtime")
				return utils.RequeueIfError(err)
			}
		}
	}

	// 3. If runtime is not deleted, then requeue
	if !deleted {
		log.Info("The runtime is deleting.")
		return utils.RequeueAfterInterval(time.Duration(1 * time.Second))
	}

	// 4. Remove finalizer
	if !ctx.Dataset.ObjectMeta.GetDeletionTimestamp().IsZero() {
		ctx.Dataset.ObjectMeta.Finalizers = utils.RemoveString(ctx.Dataset.ObjectMeta.Finalizers, common.AlluxioDatasetResourceFinalizerName)
		if err := r.Update(ctx, &ctx.Dataset); err != nil {
			log.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
		ctx.Log.V(1).Info("Finalizer is removed", "dataset", ctx.Dataset)
	}

	return ctrl.Result{}, nil
}

func (r *DatasetReconciler) addFinalizerAndRequeue(ctx reconcileRequestContext) (ctrl.Result, error) {
	ctx.Dataset.ObjectMeta.Finalizers = append(ctx.Dataset.ObjectMeta.Finalizers, common.AlluxioDatasetResourceFinalizerName)
	ctx.Log.Info("Add finalizer and Requeue")
	prevGeneration := ctx.Dataset.ObjectMeta.GetGeneration()
	if err := r.Update(ctx, &ctx.Dataset); err != nil {
		ctx.Log.Error(err, "Failed to add finalizer", "StatusUpdateError", ctx)
		return utils.RequeueIfError(err)
	}

	return utils.RequeueImmediatelyUnlessGenerationChanged(prevGeneration, ctx.Dataset.ObjectMeta.GetGeneration())
}

func (r *DatasetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&datav1alpha1.Dataset{}).
		// Watches(&source.Kind{Type: &v1.Node{}}, &handler.EnqueueRequestForObject{}).
		// WithEventFilter(&NodeStatusChangedPredicate{}).
		Complete(r)
}
