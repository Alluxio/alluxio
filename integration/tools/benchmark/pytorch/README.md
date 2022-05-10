# Pytorch Data Loading Benchmark

This module includes the testing scripts for benchmarking Pytorch data loading performance of various file system implementations including Alluxio POSIX API.

## Run single node benchmarking

- Launch the Alluxio cluster with master and worker
- Launch Alluxio Fuse to mount Alluxio namespace to host path `/mnt/alluxio-fuse/` in this benchmarking node
- download demo image: `luqqiu/alluxioloadagent:latest` which is built based on the Dockerfile included in this module
- Start docker with the following command
```
docker run -it --rm --name loadtest -e NVIDIA_VISIBLE_DEVICES= -v `pwd`:/v/ -v /mnt:/mnt:rshared -w /v luqqiu/alluxioloadagent:latest bash
```
- prepare file name list into `inputdata.csv`, one filepath per file **WITHOUT** the common alluxio path prefix.
The common path prefix will be passed to `load.py`.
```
./run-test.sh 2 load.py --workers 2 --file_name_list inputdata.csv --number_of_files 10000 \
  -p /mnt/alluxio-fuse/data/
```

## Run multi-node benchmarking

- Launch the Alluxio cluster with master and worker
- Launch Alluxio Fuse to mount Alluxio namespace to host path `/mnt/alluxio-fuse/` in each benchmarking node
- Launch the docker container in each training node
- Prepare file name list in each training docker container with name `inputdata.csv`
- Run the load script
For example, benchmarking data loading performance in two nodes.
Run the following command in node one:
```
export MASTER_ADDR=${NODE_ONE_HOSTNAME} \
&& export MASTER_PORT=${NODE_ONE_PORT} \
&& export WORLD_SIZE=2 \
&& export RANK=0 \
&& run-test.sh 2 load.py --workers 2 --file_name_list inputdata.csv --number_of_files 10000 \
-p /mnt/alluxio-fuse/data/"
```
Change the `RANK=0` to `RANK=1` and run in the other node.

## Run multi-node benchmarking with Arena

[Arena](https://github.com/kubeflow/arena) can be used for running the benchmark in multi-node.
```
arena --loglevel info submit pytorch --name=test-job --gpus=0 --workers=2 --cpu 4 --memory 32G \
--image=luqqiu/alluxioloadagent:latest --selector alluxio-master=false --data-dir=/alluxio/ \
--sync-mode=git --sync-source=https://github.com/LuQQiu/TrainingScript.git \
"export MASTER_ADDR=test-job-master-0 && export MASTER_PORT=12425 \
&& /root/code/TrainingScript/run-test.sh 2 /root/code/TrainingScript/load.py \
--workers 2 --file_name_list /root/code/TrainingScript/header-1-3m-100kb-130gb.csv --number_of_files 10000 \
-p /alluxio/alluxio-mountpoint/alluxio-fuse/dali/train/"
```
Please refer to [Distribtued Pytorch Training Guide](https://arena-docs.readthedocs.io/en/latest/training/pytorchjob/distributed/)
for more information about how to launch a pytorch script in multi-node.

Get the benchmarking pods name
```
kubectl get pods
```

The benchmark result of each node is shown in the logs of each kubernetes pod
```
kubectl logs test-job-master-0
kubectl logs test-job-worker-0
```

## Special thanks

Special thanks to Kevin Cai and Zifan Ni for contributing this benchmark scripts.
