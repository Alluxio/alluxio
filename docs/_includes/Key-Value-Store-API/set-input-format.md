```java
conf.setInputFormat(KeyValueInputFormat.class);
FileInputFormat.setInputPaths(conf, new Path("alluxio://192.168.1.200:19998/input-store"));
```
