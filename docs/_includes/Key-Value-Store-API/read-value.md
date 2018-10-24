```java
KeyValueStoreReader reader = kvs.openStore(new AlluxioURI("alluxio://192.168.1.200:19998/path/kvstore/"));
// Return "foo"
reader.get("100");
// Return null as no value associated with "300"
reader.get("300");
// Close the reader on the store
reader.close();
```
