```bash
$ bin/flink run examples/batch/WordCount.jar 
  --input alluxio://zk@zookeeper_hostname1:2181,zookeeper_hostname2:2181,zookeeper_hostname3:2181/LICENSE 
  --output alluxio://zk@zookeeper_hostname1:2181,zookeeper_hostname2:2181,zookeeper_hostname3:2181/output
```
