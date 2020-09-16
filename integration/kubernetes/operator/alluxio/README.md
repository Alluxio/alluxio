

### Build the binary

1. Download the git repo

```
git clone https://github.com/Alluxio/alluxio.git
```

2. Copy to the Gopath

```
mkdir -p /go/src/github.com/Alluxio/
cp  -rf /tmp/alluxio/integration/kubernetes/operator/alluxio /go/src/github.com/Alluxio/
```

3. Build the binary

```
cd /go/src/github.com/Alluxio/
make
```


### Build the image

1. Build docker image

```
make docker-build-webhook
```

2. Push docker image

```
make push-webhook-image
```

### Deploy

0. Deploy CRD

```
kustomize build config/crd | kubectl apply -f -
```


1. Create namespace `alluxio-system` in which the container injector webhook is deployed:

```
kubectl create ns alluxio-system
```


2. Create a signed cert/key pair and store it in a Kubernetes secret that will be consumed by injector deployment:

```
/go/src/github.com/Alluxio/alluxio/cmd/webhook/webhook-create-signed-cert.sh     --service alluxio-webhook     --secret alluxio-webhook-certs     --namespace alluxio-system
```

3. Patch the MutatingWebhookConfiguration by set caBundle with correct value from Kubernetes cluster:

```
cat /go/src/github.com/Alluxio/alluxio/deploy/webhook/manifests.yaml | \
    /go/src/github.com/Alluxio/alluxio/cmd/webhook/webhook-patch-ca-bundle.sh > \
    /tmp/mutatingwebhook-ca-bundle.yaml
```

4. Deploy resources:

```
#kubectl create ns alluxio-system
kubectl create -f /go/src/github.com/Alluxio/alluxio/deploy/role-binding.yaml
kubectl apply -f /go/src/github.com/Alluxio/alluxio/config/webhook/webhook.yaml
kubectl apply -f /go/src/github.com/Alluxio/alluxio/config/webhook/service.yaml
kubectl apply -f /go/src/github.com/Alluxio/alluxio/config/manager/manager.yaml
kubectl apply -f /tmp/mutatingwebhook-ca-bundle.yaml
kubectl get mutatingwebhookconfigurations
```


### Verify


```
kubectl run alpine --image=alpine --restart=Never  --overrides='{"apiVersion":"v1","metadata":{"annotations":{"data.alluxio.io/dataset":"imagenet"}}}' --command -- sleep infinity
```

```
kubectl describe po alpine
```


### Clean up

```
kubectl delete po alpine
kubectl delete ns alluxio-system
kubectl delete clusterrolebindings alluxio-cluster-rolebinding
kubectl delete mutatingwebhookconfigurations alluxio-mutating-configuration
kustomize build config/crd | kubectl delete -f -
```

Reference: https://github.com/cheyang/knsk
