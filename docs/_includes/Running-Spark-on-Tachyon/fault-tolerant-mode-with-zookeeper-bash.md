```bash
export SPARK_JAVA_OPTS="
  -Dtachyon.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181 \
  -Dtachyon.zookeeper.enabled=true \
  $SPARK_JAVA_OPTS
"
```
