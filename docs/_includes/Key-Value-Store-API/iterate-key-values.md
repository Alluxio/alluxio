```java
KeyValueStoreReader reader = kvs.openStore(new AlluxioURI("alluxio://192.168.1.200:19998/path/kvstore/"));
KeyValueIterator iterator = reader.iterator();
while (iterator.hasNext()) {
  KeyValuePair pair = iterator.next();
  ByteBuffer key = pair.getkKey();
  ByteBuffer value = pair.getValue();
}
// Close the reader on the store
reader.close()
```
