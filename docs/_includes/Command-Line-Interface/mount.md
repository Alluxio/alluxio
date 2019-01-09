```bash
$ ./bin/alluxio fs mount /mnt/hdfs hdfs://host1:9000/data/
$ ./bin/alluxio fs mount --shared --readonly /mnt/hdfs2 hdfs://host2:9000/data/
$ ./bin/alluxio fs mount --option aws.accessKeyId=<accessKeyId> --option aws.secretKey=<secretKey>\
  /mnt/s3 s3a://data-bucket/
```
