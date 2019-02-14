```bash
# Shows the size information of all the files in root directory
$ ./bin/alluxio fs du /
File Size     In Alluxio       Path
1337          0 (0%)           /alluxio-site.properties
4352          4352 (100%)      /testFolder/NOTICE
26847         0 (0%)           /testDir/LICENSE
2970          2970 (100%)      /testDir/README.md

# Shows the in memory size information
$ ./bin/alluxio fs du --memory /
File Size     In Alluxio       In Memory        Path
1337          0 (0%)           0 (0%)           /alluxio-site.properties
4352          4352 (100%)      4352 (100%)      /testFolder/NOTICE
26847         0 (0%)           0 (0%)           /testDir/LICENSE
2970          2970 (100%)      2970 (100%)      /testDir/README.md

# Shows the aggregate size information in human-readable format
./bin/alluxio fs du -h -s /
File Size     In Alluxio       In Memory        Path
34.67KB       7.15KB (20%)     7.15KB (20%)     /

# Can be used to detect which folders are taking up the most space
$ ./bin/alluxio fs du -h -s /\\*
File Size     In Alluxio       Path
1337B         0B (0%)          /alluxio-site.properties
29.12KB       2970B (9%)       /testDir
4352B         4352B (100%)     /testFolder
```
