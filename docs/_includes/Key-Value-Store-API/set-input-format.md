```java
conf.setInputFormat(KeyValueInputFormat.class);
FileInputFormat.setInputPaths(conf, new Path("alluxio://input-store"));
```