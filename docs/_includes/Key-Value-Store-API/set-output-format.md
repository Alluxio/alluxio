```java
conf.setOutputKeyClass(BytesWritable.class);
conf.setOutputValueClass(BytesWritable.class);
conf.setOutputFormat(KeyValueOutputFormat.class);
FileOutputFormat.setOutputPath(conf, new Path("alluxio://192.168.1.200:19998/output-store"));
```
