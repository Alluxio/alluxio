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

	zapOpt "go.uber.org/zap"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	// +kubebuilder:scaffold:imports
	// "sigs.k8s.io/controller-runtime/pkg/webhook/admission/builder"
)

var (
	// scheme   = runtime.NewScheme()
	setupLog                  = ctrl.Log.WithName("setup")
	alluxioInitContainerImage = ""
)

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var development bool
	var port int
	flag.StringVar(&metricsAddr, "metrics-addr", ":38081", "The address the metric endpoint binds to.")
	flag.StringVar(&alluxioInitContainerImage, "mount-ready-image", "alluxio/alluxio-mount", "The container image used to check the mount is ready.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&development, "development", true,
		"Enable development mode for pillar webhook.")
	flag.IntVar(&port, "port", 9443, "The port of the webhook")
	flag.Parse()

	ctrl.SetLogger(zap.New(func(o *zap.Options) {
		o.Development = development
	}, func(o *zap.Options) {
		o.ZapOpts = append(o.ZapOpts, zapOpt.AddCaller())
	}))

	// mgr, err := manager.New(ctrl.GetConfigOrDie(), options)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		// Scheme:             scheme,
		MetricsBindAddress: metricsAddr,
		LeaderElection:     enableLeaderElection,
		Port:               port,
		CertDir:            "/tmp/cert",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Setup webhooks
	setupLog.Info("setting up webhook server")
	hookServer := mgr.GetWebhookServer()

	setupLog.Info("registering webhooks to the webhook server")
	hookServer.Register("/mutate-alluxio-pod", &webhook.Admission{Handler: &PodCreateHandler{Client: mgr.GetClient(),
		Log: ctrl.Log.WithName("webhook")}})

	// ns := os.Getenv("POD_NAMESPACE")
	// if len(ns) == 0 {
	//     ns = "alluxio-system"
	// }
	// secretName := os.Getenv("SECRET_NAME")
	// if len(secretName) == 0 {
	//     secretName = "alluxio-webhook-server-secret"
	// }

	// bootstrapOptions := &webhook.BootstrapOptions{
	//     MutatingWebhookConfigName:   "alluxio-mutating-webhook-configuration",
	//     // ValidatingWebhookConfigName: "alluxio-validating-webhook-configuration",
	// }

	// if webhookHost := os.Getenv("WEBHOOK_HOST"); len(webhookHost) > 0 {
	//     bootstrapOptions.Host = &webhookHost
	// } else {
	//     bootstrapOptions.Service = &webhook.Service{
	//         Namespace: ns,
	//         Name:      "alluxio-webhook-server-service",
	//         // Selectors should select the pods that runs this webhook server.
	//         Selectors: map[string]string{
	//             "control-plane": "webhook-manager",
	//         },
	//     }
	//     bootstrapOptions.Secret = &types.NamespacedName{
	//         Namespace: ns,
	//         Name:      secretName,
	//     }
	// }

	// svr, err := webhook.NewServer("kruise-admission-server", mgr, webhook.ServerOptions{
	//     Port:             webhookPort,
	//     CertDir:          "/tmp/cert",
	//     BootstrapOptions: bootstrapOptions,
	// })
	// if err != nil {
	//     setupLog.Error(err, "unable to start manager")
	//     os.Exit(1)
	// }

	// builderName := "mutating-create-pod-alluxio"
	// wh, err := builder.NewWebhookBuilder().
	//     Name(builderName + ".alluxio.io").
	//     Path("/" + builderName).
	//     Mutating().
	//     Operations(admissionregistrationv1beta1.Create).
	//     FailurePolicy(admissionregistrationv1beta1.Fail).
	//     ForType(&corev1.Pod{}).Handlers(&PodCreateHandler{}).
	// WithManager(mgr).
	// Build()
	// if err != nil{
	//    setupLog.Error(err, "unable to start manager")
	//    os.Exit(1)
	// }

	// err = svr.Register(wh)
	// if err != nil{
	//    setupLog.Error(err, "unable to start manager")
	//    os.Exit(1)
	// }

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
