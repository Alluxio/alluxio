<!---
NOTE: This code is tested in LineageMasterIntegrationTest, so if you update it here make sure to
update it there as well.
-->

```java
AlluxioLineage tl = AlluxioLineage.get();
// input file paths
AlluxioURI input1 = new AlluxioURI("/inputFile1");
AlluxioURI input2 = new AlluxioURI("/inputFile2");
List<AlluxioURI> inputFiles = Lists.newArrayList(input1, input2);
// output file paths
AlluxioURI output = new AlluxioURI("/outputFile");
List<AlluxioURI> outputFiles = Lists.newArrayList(output);
// command-line job
JobConf conf = new JobConf("/tmp/recompute.log");
CommandLineJob job = new CommandLineJob("my-spark-job.sh", conf);
long lineageId = tl.createLineage(inputFiles, outputFiles, job);
```
