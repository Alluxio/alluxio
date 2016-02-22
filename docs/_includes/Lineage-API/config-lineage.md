<!---
NOTE: This code is tested in LineageMasterIntegrationTest, so if you update it here make sure to
update it there as well.
-->

```java
TachyonLineage tl = TachyonLineage.get();
// input file paths
TachyonURI input1 = new TachyonURI("/inputFile1");
TachyonURI input2 = new TachyonURI("/inputFile2");
List<TachyonURI> inputFiles = Lists.newArrayList(input1, input2);
// output file paths
TachyonURI output = new TachyonURI("/outputFile");
List<TachyonURI> outputFiles = Lists.newArrayList(output);
// command-line job
JobConf conf = new JobConf("/tmp/recompute.log");
CommandLineJob job = new CommandLineJob("my-spark-job.sh", conf);
long lineageId = tl.createLineage(inputFiles, outputFiles, job);
```
