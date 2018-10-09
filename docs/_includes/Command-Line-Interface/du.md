```bash
# Shows the size information of all the files in root directory
$ ./bin/alluxio fs du /
4352          4352 (100%)      /example2/NOTICE
26847         0 (0%)           /example/LICENSE
2970          2970 (100%)      /example/README.md

# Shows the header and the in memory information
$ ./bin/alluxio fs du --header --memory /
File Size     In Alluxio       In Memory        Path
4352          4352 (100%)      4352 (100%)      /example2/NOTICE
26847         0 (0%)           0 (0%)           /example/LICENSE
2970          2970 (100%)      2970 (100%)      /example/README.md

# Shows the aggregate size information in human-readable format
./bin/alluxio fs du -s -h /
33.37KB       7.15KB (21%)     /

# Shows the aggregate size information of all the child directories of root
$ ./bin/alluxio fs du -s /\\*
29817         2970 (9%)        /example
4352          4352 (100%)      /example2
```
