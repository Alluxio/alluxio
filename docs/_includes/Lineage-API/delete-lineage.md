```java
TachyonLineage tl = TachyonLineage.get();
...
long lineageId = tl.createLineage(inputFiles, outputFiles, job);
...
tl.deleteLineage(lineageId);
```
