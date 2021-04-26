# Readme
This directory contains some docker files to help you set up metrics tracking on an alluxio cluster. 
The scripts uses opentelemetry's auto instrumentation java agent to generate tracing information for GRPC and S3 calls, which is visualized using Jaeger and Prometheus. 

For reference, please refer to documentations listed at [opentelemetry GitHub page](https://github.com/open-telemetry/opentelemetry-java-instrumentation)

1. On one of the Alluxio master or the node where you want to run Jaeger and Prometheus, run 
```
docker-compose -f docker-compose-master.yaml run -e MASTER_IP=xxx.xx.xx.xx -d
```
2. On all other alluxio nodes, run 
```
docker-compose -f docker-compose-worker.yaml run -e MASTER_IP=xxx.xx.xx.xx -d
```
3. Download the auto instrumentation jar and place it in the `alluxio/conf` directory. The jar can be found at 
```
https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent-all.jar
```
4. Edit `conf/alluxio-env.sh` and add
    
    ```
ALLUXIO_MASTER_JAVA_OPTS+=" -javaagent:./conf/opentelemetry-javaagent-all.jar \
     -Dotel.resource.attributes=service.name=AlluxioMaster \
     "
ALLUXIO_WORKER_JAVA_OPTS+=" -javaagent:./conf/opentelemetry-javaagent-all.jar \
     -Dotel.resource.attributes=service.name=AlluxioWorker \
     "
    ```
5. Copy the content of `alluxio/conf` dir to all nodes using alluxio utility.
    ```
bin/alluxio copyDir conf
    ```
6. Restart alluxio master and workers
7. Point your browser to `MASTER_IP:16686` for tracing and `MASTER_IP:9090` for metrics
