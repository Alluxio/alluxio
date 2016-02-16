```java
TachyonLineage tl = TachyonLineage.get();
DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(true);
tl.deleteLineage(lineageId, options);
```
