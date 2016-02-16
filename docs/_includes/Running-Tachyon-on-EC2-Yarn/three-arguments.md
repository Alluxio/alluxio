```bash
$ export HADOOP_HOME=/hadoop
$ /hadoop/bin/hadoop fs -mkdir hdfs://TachyonMaster:9000/tmp
$ /tachyon/integration/bin/tachyon-yarn.sh 3 hdfs://TachyonMaster:9000/tmp/
```
