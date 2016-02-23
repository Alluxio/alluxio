```java
AlluxioLineage tl = AlluxioLineage.get();
...
long lineageId = tl.createLineage(inputFiles, outputFiles, job);
...
tl.deleteLineage(lineageId);
```
