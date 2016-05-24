```bash
$ export ALLUXIO_MASTER_HOSTNAME="localhost"
$ export ALLUXIO_UNDERFS_ADDRESS="hdfs://localhost:9000"
$ export ALLUXIO_MASTER_JAVA_OPTS="$ALLUXIO_JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=7001“
```
