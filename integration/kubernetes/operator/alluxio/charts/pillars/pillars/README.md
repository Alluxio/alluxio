# alluxio

## Install

1. Download and extract the package

```
wget http://kubeflow.oss-cn-beijing.aliyuncs.com/alluxio-0.2.0.tgz
tar -xvf alluxio-0.2.0.tgz
```

2. Create a signed cert/key pair and store it in a Kubernetes secret that will be consumed by injector deployment

```
bash alluxio/scripts/webhook-create-signed-cert.sh --service alluxio-webhook --secret alluxio-webhook-certs --namespace alluxio-system
```

3. Deploy

```
helm install alluxio alluxio --set controller.image=alluxio/alluxio-controller:v0.1.0-9113c3b,webhook.image=alluxio/alluxio-webhook:v0.1.0-9113c3b,mount.image=alluxio/alluxio-mount
```

you will see follow:

```
NAME: alluxio
LAST DEPLOYED: Wed May 13 20:19:54 2020
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
```

4. Patch the MutatingWebhookConfiguration by set caBundle with correct value from Kubernetes cluster:

```
cat alluxio/scripts/webhookconfiguration.yaml | \
    bash alluxio/scripts/webhook-patch-ca-bundle.sh > \
    /tmp/mutatingwebhook-ca-bundle.yaml
kubectl apply -f /tmp/mutatingwebhook-ca-bundle.yaml
```

5. Create default configuration for alluxio-engine

```
kubectl create ns alluxio-system
cp alluxio/advanced.yaml advanced.yaml
kubectl create cm default-alluxio-template --namespace alluxio-system --from-file=data=advanced.yaml
```

6. Use the follow yaml to create dataset

```
apiVersion: data.alluxio.io/v1alpha1
kind: Dataset
metadata:
  name: mydata
  namespace: default
spec:
  mounts:
  - mountPoint: oss://imagenet-huabei5/
    name: imagenet
    options:
      fs.oss.accessKeyId: xxx
      fs.oss.accessKeySecret: yyy
      fs.oss.endpoint: oss-cn-huhehaote-internal.aliyuncs.com
  - mountPoint: oss://coco-6g/
    name: coco
    options:
      fs.oss.accessKeyId: xxx
      fs.oss.accessKeySecret: yyy
      fs.oss.endpoint: oss-cn-huhehaote-internal.aliyuncs.com
  nodeAffinity:
    required:
       nodeSelectorTerms:
          - matchExpressions:
            - key: aliyun.accelerator/nvidia_name
              operator: In
              values:
              - Tesla-P100-PCIE-16GB
  prefetchStrategy: Never
  replicas: 1
```

## Uninstall

```
helm delete alluxio
kubectl delete -f alluxio/templates/crd/
kubectl delete mutatingwebhookconfigurations alluxio-mutating-configuration
```
