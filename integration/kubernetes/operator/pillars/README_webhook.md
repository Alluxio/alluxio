

### Build

1. Build docker image

```
make docker-build-webhook
```

2. Push docker image

```
make push-webhook-image
```


### Prepare

```
cd /tmp
wget http://kubeflow.oss-cn-beijing.aliyuncs.com/kustomize_v3.5.4_linux_amd64.tar.gz
tar -xvf kustomize_v3.5.4_linux_amd64.tar.gz
cp kustomize /usr/local/bin/
```


### Deploy

0. Deploy CRD

```
kustomize build config/crd | kubectl apply -f -
```


1. Create namespace `pillars-system` in which the container injector webhook is deployed:

```
kubectl create ns pillars-system
```


2. Create a signed cert/key pair and store it in a Kubernetes secret that will be consumed by injector deployment:

```
/go/src/github.com/Alluxio/pillars/cmd/webhook/webhook-create-signed-cert.sh     --service pillars-webhook     --secret pillars-webhook-certs     --namespace pillars-system
```

3. Patch the MutatingWebhookConfiguration by set caBundle with correct value from Kubernetes cluster:

```
cat /go/src/github.com/Alluxio/pillars/deploy/webhook/manifests.yaml | \
    /go/src/github.com/Alluxio/pillars/cmd/webhook/webhook-patch-ca-bundle.sh > \
    /tmp/mutatingwebhook-ca-bundle.yaml
```

4. Deploy resources:

```
#kubectl create ns pillars-system
kubectl create -f /go/src/github.com/Alluxio/pillars/deploy/role-binding.yaml
kubectl apply -f /go/src/github.com/Alluxio/pillars/config/webhook/webhook.yaml
kubectl apply -f /go/src/github.com/Alluxio/pillars/config/webhook/service.yaml
kubectl apply -f /go/src/github.com/Alluxio/pillars/config/manager/manager.yaml
kubectl apply -f /tmp/mutatingwebhook-ca-bundle.yaml
kubectl get mutatingwebhookconfigurations
```


### Verify


```
kubectl run alpine --image=alpine --restart=Never  --overrides='{"apiVersion":"v1","metadata":{"annotations":{"data.pillars.io/dataset":"imagenet"}}}' --command -- sleep infinity
```

```
kubectl describe po alpine
```


### Clean up

```
kubectl delete po alpine
kubectl delete ns pillars-system
kubectl delete clusterrolebindings pillars-cluster-rolebinding
kubectl delete mutatingwebhookconfigurations pillars-mutating-configuration
kustomize build config/crd | kubectl delete -f -
```

Reference: https://github.com/cheyang/knsk