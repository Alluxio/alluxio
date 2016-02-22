```java
KeyValueStoreReader reader = kvs.openStore(new AlluxioURI("alluxio://path/kvstore/"));
KeyValueIterator iterator = reader.iterator();
while (iterator.hasNext()) {
  KeyValuePair pair = iterator.next();
  ByteBuffer key = pair.getkKey();
  ByteBuffer value = pair.getValue();
}
// Close the reader on the store
reader.close()
```