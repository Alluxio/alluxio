```java
KeyValueStoreWriter writer = kvs.createStore(new AlluxioURI("alluxio://path/my-kvstore"));
// Insert key-value pair ("100", "foo")
writer.put("100", "foo");
// Insert key-value pair ("200", "bar")
writer.put("200", "bar");
// Close and complete the store
writer.close();
```
