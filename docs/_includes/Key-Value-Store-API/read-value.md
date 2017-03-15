```java
KeyValueStoreReader reader = kvs.openStore(new AlluxioURI("alluxio://path/kvstore/"));
// Return "foo"
reader.get("100");
// Return null as no value associated with "300"
reader.get("300");
// Close the reader on the store
reader.close();
```
