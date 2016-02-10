```java
conf.setInputFormat(KeyValueInputFormat.class);
FileInputFormat.setInputPaths(conf, new Path("tachyon://input-store"));
```