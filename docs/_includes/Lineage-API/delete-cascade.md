```java
AlluxioLineage tl = AlluxioLineage.get();
DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(true);
tl.deleteLineage(lineageId, options);
```
