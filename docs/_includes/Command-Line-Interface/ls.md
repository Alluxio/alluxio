```bash
$ ./bin/alluxio fs mount /s3/data s3n://data-bucket/
# Loads metadata for all immediate children of /s3/data and lists them.
$ ./bin/alluxio fs ls /s3/data/
#
# Forces loading metadata.
$ aws s3 cp /tmp/somedata s3n://data-bucket/somedata
$ ./bin/alluxio fs ls -f /s3/data 
#
# Files are not removed from Alluxio if they are removed from the UFS (s3 here) only.
$ aws s3 rm s3n://data-bucket/somedata
$ ./bin/alluxio fs ls -f /s3/data
```
