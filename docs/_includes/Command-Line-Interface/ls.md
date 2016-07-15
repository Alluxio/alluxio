```bash
$ ./bin/alluxio fs mount s3n://data-bucket/ /s3/data
# Loads metadata for all immediate children of /s3/data and lists them
$ ./bin/alluxio fs ls /s3/data/
#
# Force loading metadata.
$ aws s3 cp /tmp/somedata s3n://data-bucket/somedata
$ ./bin/alluxio fs ls -f /s3/data 
```
