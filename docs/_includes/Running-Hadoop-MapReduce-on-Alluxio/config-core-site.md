```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
  <description>The Alluxio FileSystem (Hadoop 1.x & 2.x)</description>
</property>
<property>
  <name>fs.alluxio-ft.impl</name>
  <value>alluxio.hadoop.FaultTolerantFileSystem</value>
  <description>The Alluxio FileSystem (Hadoop 1.x & 2.x) with fault tolerant support</description>
</property>
<property>
  <name>fs.AbstractFileSystem.alluxio.impl</name>
  <value>alluxio.hadoop.AlluxioFs</value>
  <description>The Alluxio AbstractFileSystem (Hadoop 2.x)</description>
</property>
```
