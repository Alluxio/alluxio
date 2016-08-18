```java
conf.setOutputKeyClass(BytesWritable.class);
conf.setOutputValueClass(BytesWritable.class);
conf.setOutputFormat(KeyValueOutputFormat.class);
FileOutputFormat.setOutputPath(conf, new Path("alluxio://output-store"));
```
