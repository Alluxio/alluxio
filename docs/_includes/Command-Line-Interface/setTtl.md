```bash
# After 1 day, delete the file in Alluxio and UFS
$ ./bin/alluxio fs setTtl /data/good-for-one-day 86400000
# After 1 day, free the file from Alluxio
$ ./bin/alluxio fs setTtl --action free /data/good-for-one-day 86400000
```
