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
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/Alluxio/alluxio/api/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/ddc"
	"github.com/Alluxio/alluxio/pkg/ddc/base"
	"github.com/Alluxio/alluxio/pkg/ddc/configs"
	"github.com/Alluxio/alluxio/pkg/utils"
)

// RuntimeReconciler reconciles a Runtime object
type RuntimeReconciler struct {
	client.Client
	Log          logr.Logger
	Recorder     record.EventRecorder
	Scheme       *runtime.Scheme
	ResyncPeriod time.Duration
	Engines      map[string]base.Engine
}

// +kubebuilder:rbac:groups=data.alluxio.io,resources=runtimes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=data.alluxio.io,resources=runtimes/status,verbs=get;update;patch

func (r *RuntimeReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := common.ReconcileRequestContext{
		Context:        context.Background(),
		Log:            r.Log.WithValues("runtime", req.NamespacedName),
		NamespacedName: req.NamespacedName,
	}

	ctx.Log.V(1).Info("process the request", "request", req)

	/*
	 *	###1. Load the Runtime
	 */
	runtime, err := r.getRuntime(ctx)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			ctx.Log.V(1).Info("The runtime is not found", "runtime", ctx.NamespacedName)
			return ctrl.Result{}, nil
		} else {
			ctx.Log.Error(err, "Failed to get the ddc runtime")
			return utils.RequeueIfError(errors.Wrap(err, "Unable to get ddc runtime"))
		}
	}

	ctx.Runtime = runtime

	// 1.Reconcile delete the runtime
	if !ctx.Runtime.ObjectMeta.GetDeletionTimestamp().IsZero() {
		return r.reconcileRuntimeDeletion(ctx)
	}

	if !utils.ContainsString(ctx.Runtime.ObjectMeta.GetFinalizers(), common.AlluxioRuntimeResourceFinalizerName) {
		return r.addFinalizerAndRequeue(ctx)
	}

	// 2.Get the Dataset
	dataset, err := r.getDataset(ctx)
	if err != nil {
		// r.Recorder.Eventf(ctx.Dataset, corev1.EventTypeWarning, common.ErrorProcessRuntimeReason, "Process Runtime error %v", err)
		if utils.IgnoreNotFound(err) == nil {
			ctx.Log.V(1).Info("The dataset is not found", "dataset", ctx.NamespacedName)
			return ctrl.Result{}, nil
		} else {
			ctx.Log.Error(err, "Failed to get the ddc dataset")
			return utils.RequeueIfError(errors.Wrap(err, "Unable to get dataset"))
		}
	}

	ctx.Dataset = dataset

	// 3.Reconcile runtime
	return r.reconcileRuntime(ctx)
}

// reconcile Runtime
func (r *RuntimeReconciler) reconcileRuntime(ctx common.ReconcileRequestContext) (ctrl.Result, error) {
	log := ctx.Log.WithName("reconcileRuntime")
	log.V(1).Info("process the Runtime", "Runtime", ctx.Runtime)
	var err error = nil

	// 1.Get config from dataset and runtime
	config := r.buildConfig(ctx)
	key := fmt.Sprintf("%s/%s",
		ctx.NamespacedName.Namespace,
		ctx.NamespacedName.Name)
	engine, err := r.getOrCreateEngine(key, config, r.Client, r.Log)
	if err != nil {
		r.Recorder.Eventf(ctx.Runtime, corev1.EventTypeWarning, common.ErrorProcessRuntimeReason, "Process Runtime error %v", err)
		return utils.RequeueIfError(errors.Wrap(err, "Failed to build config"))
	}

	// 2.Setup the ddc engine, and wait it ready
	ready, err := engine.Setup(ctx)
	if err != nil {
		r.Recorder.Eventf(ctx.Runtime, corev1.EventTypeWarning, common.ErrorProcessRuntimeReason, "Failed to setup ddc engine due to error %v", err)
		log.Error(err, "Failed to steup the ddc engine")
		// return utils.RequeueIfError(errors.Wrap(err, "Failed to steup the ddc engine"))
	}
	if !ready {
		return utils.RequeueAfterInterval(time.Duration(10 * time.Second))
	}

	// 3. Check the health of the runtime

	err = engine.HealthyCheck()
	if err != nil {
		r.Recorder.Eventf(ctx.Runtime, corev1.EventTypeWarning, common.ErrorProcessRuntimeReason, "Failed to sync up the status of the ddc due to %v", err)
		r.Recorder.Eventf(ctx.Dataset, corev1.EventTypeWarning, common.ErrorProcessDatasetReason,
			"The ddc runtime is not ready, please check by using 'kubectl get runtime %s -n %s'", ctx.Dataset.Name, ctx.Dataset.Namespace)
		return utils.RequeueAfterInterval(time.Duration(10 * time.Second))
	}

	// 4. Check if the replicas is changed
	err = engine.SyncReplicas(ctx.Dataset.Spec.Replicas)
	if err != nil {
		r.Recorder.Eventf(ctx.Runtime, corev1.EventTypeWarning, common.ErrorProcessRuntimeReason, "Failed to sync the replicas of the ddc due to %v", err)
		return utils.RequeueAfterInterval(time.Duration(10 * time.Second))
	}

	// 5. Check if need to scale out or in nodes
	_, err = engine.SyncNodes()
	if err != nil {
		r.Recorder.Eventf(ctx.Runtime, corev1.EventTypeWarning, common.ErrorProcessRuntimeReason, "Failed to sync the nodes of the ddc due to %v", err)
		return utils.RequeueAfterInterval(time.Duration(10 * time.Second))
	}

	// 6. Sync up with UFS
	_, err = engine.SyncUFS()
	if err != nil {
		r.Recorder.Eventf(ctx.Runtime, corev1.EventTypeWarning, common.ErrorProcessRuntimeReason, "Failed to sync the UFS due to %v", err)
		return utils.RequeueAfterInterval(time.Duration(10 * time.Second))
	}

	// 7. Decide if it needs load data
	if ctx.Dataset.Spec.PrefetchStrategy == datav1alpha1.AlwaysPrefetch {
		// needLoaded := false

		// _, err := r.UpdateCacheStates(ctx, engine)
		// if err != nil {
		// 	return utils.RequeueAfterInterval(time.Duration(10 * time.Second))
		// }

		err = engine.LoadData()
		if err != nil {
			r.Log.Error(err, "Failed to load the data")
			return utils.RequeueAfterInterval(time.Duration(10 * time.Second))
		}

	} else {
		err = engine.UpdateCacheStateOfDataset()
		if err != nil {
			r.Log.Error(err, "Failed to update the status of data")
			return utils.RequeueAfterInterval(time.Duration(10 * time.Second))
		}
	}

	// return utils.RequeueAfterInterval(r.ResyncPeriod)
	return utils.RequeueAfterInterval(time.Duration(15 * time.Second))
}

// reconcile Runtime Deletion
func (r *RuntimeReconciler) reconcileRuntimeDeletion(ctx common.ReconcileRequestContext) (ctrl.Result, error) {
	log := ctx.Log.WithName("reconcileRuntimeDeletion")
	log.V(1).Info("process the Runtime Deletion", "Runtime", ctx.Runtime)

	// 1. Delete the implementation of the the runtime
	config := r.buildConfig(ctx)
	key := fmt.Sprintf("%s/%s",
		ctx.NamespacedName.Namespace,
		ctx.NamespacedName.Name)
	engine, err := r.getOrCreateEngine(key, config, r.Client, r.Log)
	if err != nil {
		r.Recorder.Eventf(ctx.Runtime, corev1.EventTypeWarning, common.ErrorProcessRuntimeReason, "Process Runtime error %v", err)
		return utils.RequeueIfError(errors.Wrap(err, "Failed to build config"))
	}

	err = engine.Destroy()
	if err != nil {
		r.Recorder.Eventf(ctx.Runtime, corev1.EventTypeWarning, common.ErrorProcessRuntimeReason, "Failed to destroy engine %v", err)
		return utils.RequeueIfError(errors.Wrap(err, "Failed to destroy the engine"))
	}

	r.removeEngine(key)

	// 2. Remove finalizer
	if !ctx.Runtime.ObjectMeta.GetDeletionTimestamp().IsZero() {
		ctx.Runtime.ObjectMeta.Finalizers = utils.RemoveString(ctx.Runtime.ObjectMeta.Finalizers, common.AlluxioRuntimeResourceFinalizerName)
		if err := r.Update(ctx, ctx.Runtime); err != nil {
			log.Error(err, "Failed to remove finalizer")
			return utils.RequeueIfError(err)
		}
		ctx.Log.V(1).Info("Finalizer is removed", "runtime", ctx.Runtime)
	}

	return ctrl.Result{}, nil
}

func (r *RuntimeReconciler) addFinalizerAndRequeue(ctx common.ReconcileRequestContext) (ctrl.Result, error) {
	ctx.Runtime.ObjectMeta.Finalizers = append(ctx.Runtime.ObjectMeta.Finalizers, common.AlluxioRuntimeResourceFinalizerName)
	ctx.Log.Info("Add finalizer and Requeue")
	prevGeneration := ctx.Runtime.ObjectMeta.GetGeneration()
	if err := r.Update(ctx, ctx.Runtime); err != nil {
		ctx.Log.Error(err, "Failed to add finalizer", "StatusUpdateError", ctx)
		return utils.RequeueIfError(err)
	}

	return utils.RequeueImmediatelyUnlessGenerationChanged(prevGeneration, ctx.Runtime.ObjectMeta.GetGeneration())
}

func (r *RuntimeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&datav1alpha1.Runtime{}).
		Complete(r)
}

func (r *RuntimeReconciler) getOrCreateEngine(id string,
	config *configs.Config,
	client client.Client,
	log logr.Logger) (engine base.Engine, err error) {

	found := false

	if engine, found = r.Engines[id]; !found {
		engine, err = ddc.CreateEngine(id,
			config,
			client,
			log)
		if err != nil {
			return nil, err
		}
		r.Engines[id] = engine
	}

	return engine, err
}

func (r *RuntimeReconciler) removeEngine(id string) {
	delete(r.Engines, id)
}
