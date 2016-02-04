```bash
$ export TACHYON_UNDERFS_ADDRESS="hdfs://localhost:9000"
$ export TACHYON_MASTER_JAVA_OPTS="$TACHYON_JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=7001“
```
