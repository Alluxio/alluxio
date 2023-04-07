


1. Run the pod

```
kubectl run alpine --image=cheyang/alpine:3.11-rsync --restart=Never  --overrides='{"apiVersion":"v1","metadata":{"annotations":{"data.alluxio.io/dataset":"cifar10"}}}' --command -- sleep infinity
```

1. First time

```
kubectl exec -it alpine bash
time rsync --progress /dataset/http/cuda-cufft-dev-10-2-10.2.89-1.x86_64.rpm /tmp
cuda-cufft-dev-10-2-10.2.89-1.x86_64.rpm
    215,988,004 100%  452.51kB/s    0:07:46 (xfr#1, to-chk=0/1)

real	7m46.392s
user	0m0.890s
sys	0m0.271s
```

2. Second time

```
kubectl exec -it alpine bash
bash-5.0# rm -f /tmp/cuda-cufft-dev-10-2-10.2.89-1.x86_64.rpm
bash-5.0# time cp /dataset/http/cuda-cufft-dev-10-2-10.2.89-1.x86_64.rpm /tmp

real	0m1.268s
user	0m0.000s
sys	0m0.530s
```
