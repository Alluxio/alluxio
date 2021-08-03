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
	"flag"
	"os"
	"time"

	datav1alpha1 "github.com/Alluxio/alluxio/api/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/common"
	datactlv1alpha1 "github.com/Alluxio/alluxio/pkg/controllers/dataset/v1alpha1"
	runtimectlv1alpha1 "github.com/Alluxio/alluxio/pkg/controllers/runtime/v1alpha1"
	"github.com/Alluxio/alluxio/pkg/ddc/base"

	zapOpt "go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	// +kubebuilder:scaffold:imports
)

const DefaultResyncPeriod = 20 * time.Second

const DefaultStoragePercentage float64 = 50

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)

	_ = datav1alpha1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

func main() {
	var (
		metricsAddr                     string
		reservedStorage                 string
		enableLeaderElection            bool
		development                     bool
		resyncPeriod                    time.Duration
		percentageOfNodeStorageCapacity float64
		cacheStoreType                  string
		cacheStoragePath                string
	)
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&development, "development", true,
		"Enable development mode for pillar controller.")
	flag.DurationVar(&resyncPeriod, "resync-period", DefaultResyncPeriod, "Resync interval of the dataset")
	flag.Float64Var(&percentageOfNodeStorageCapacity, "capacity-percentage", DefaultStoragePercentage, "Set the percentage of capacity")
	flag.StringVar(&reservedStorage, "reserved", "1GiB", "reserved storage for every node")
	flag.StringVar(&cacheStoreType, "cache-store-type", "MEM", "the cache store type is MEM or DISK")
	flag.StringVar(&cacheStoragePath, "cache-storage-path", "/dev/shm", "the path of cache storage")
	flag.Parse()
	// Set the percentage of the node storage capacity

	ctrl.SetLogger(zap.New(func(o *zap.Options) {
		o.Development = development
	}, func(o *zap.Options) {
		o.ZapOpts = append(o.ZapOpts, zapOpt.AddCaller())
	}))

	common.PercentageOfNodeStorageCapacity = percentageOfNodeStorageCapacity
	common.ReservedNodeStorageCapacity = reservedStorage
	setupLog.Info("cacheStorageType", "type", cacheStoreType)
	if cacheStoreType == "DISK" {
		setupLog.Info("Setup cacheStorageType", "type", cacheStoreType)
		if cacheStoragePath == "/dev/shm" {
			cacheStoragePath = "/var/lib/docker"
		}
	} else {
		cacheStoreType = "MEM"
		setupLog.Info("Setup cacheStorageType with default setting", "type", cacheStoreType)
	}
	common.CacheStoreType = cacheStoreType
	common.CacheStoragePath = cacheStoragePath

	setupLog.Info("Setup the DDC controller", "PercentageOfNodeStorageCapacity", percentageOfNodeStorageCapacity,
		"ReservedNodeStorageCapacity", reservedStorage,
		"cacheStoreType", cacheStoreType,
		"cacheStoragePath", cacheStoragePath)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:             scheme,
		MetricsBindAddress: metricsAddr,
		LeaderElection:     enableLeaderElection,
		Port:               9443,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = (&datactlv1alpha1.DatasetReconciler{
		Client:       mgr.GetClient(),
		Recorder:     mgr.GetEventRecorderFor("dataset-controller"),
		Log:          ctrl.Log.WithName("controllers").WithName("Dataset"),
		ResyncPeriod: resyncPeriod,
		Scheme:       mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Dataset")
		os.Exit(1)
	}

	if err = (&runtimectlv1alpha1.RuntimeReconciler{
		Client:       mgr.GetClient(),
		Recorder:     mgr.GetEventRecorderFor("runtime-controller"),
		Log:          ctrl.Log.WithName("controllers").WithName("Runtime"),
		ResyncPeriod: resyncPeriod,
		Scheme:       mgr.GetScheme(),
		Engines:      map[string]base.Engine{},
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Runtime")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
