```bash
$ export HADOOP_HOME=/hadoop
$ /hadoop/bin/hadoop fs -mkdir hdfs://AlluxioMaster:9000/tmp
$ /alluxio/integration/bin/alluxio-yarn.sh 3 hdfs://AlluxioMaster:9000/tmp/
```
