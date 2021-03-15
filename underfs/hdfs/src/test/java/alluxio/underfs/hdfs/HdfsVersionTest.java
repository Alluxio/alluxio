/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.hdfs;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link HdfsVersion}.
 */
public class HdfsVersionTest {

  @Test
  public void find() throws Exception {
    Assert.assertNull(HdfsVersion.find("NotValid"));
    for (HdfsVersion version : HdfsVersion.values()) {
      Assert.assertEquals(version, HdfsVersion.find(version.getCanonicalVersion()));
    }
  }

  @Test
  public void findByHadoopLabel() throws Exception {
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("1.0.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("1.0.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("hadoop-1.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("hadoop-1.0.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("hadoop1.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("1.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("1.2.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("hadoop-1.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("hadoop-1.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("hadoop1.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("2.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("2.2.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("hadoop-2.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("hadoop-2.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("hadoop2.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("2.3.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("2.3.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("hadoop-2.3"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("hadoop-2.3.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("hadoop2.3"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("2.4.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("2.4.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("hadoop-2.4"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("hadoop-2.4.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("hadoop2.4"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("2.5.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("2.5.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("hadoop-2.5"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("hadoop-2.5.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("hadoop2.5"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("2.6.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("2.6.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("hadoop-2.6"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("hadoop-2.6.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("hadoop2.6"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("2.7.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("2.7.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("hadoop-2.7"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("hadoop-2.7.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("hadoop2.7"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("2.8.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("2.8.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("hadoop-2.8"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("hadoop-2.8.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("hadoop2.8"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_9, HdfsVersion.find("2.9.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_9, HdfsVersion.find("2.9.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_9, HdfsVersion.find("hadoop-2.9"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_9, HdfsVersion.find("hadoop-2.9.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_9, HdfsVersion.find("hadoop2.9"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_0, HdfsVersion.find("3.0.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_0, HdfsVersion.find("3.0.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_0, HdfsVersion.find("hadoop-3.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_0, HdfsVersion.find("hadoop-3.0.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_0, HdfsVersion.find("hadoop3.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_1, HdfsVersion.find("3.1.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_1, HdfsVersion.find("3.1.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_1, HdfsVersion.find("hadoop-3.1"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_1, HdfsVersion.find("hadoop-3.1.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_1, HdfsVersion.find("hadoop3.1"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_2, HdfsVersion.find("3.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_2, HdfsVersion.find("3.2.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_2, HdfsVersion.find("hadoop-3.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_2, HdfsVersion.find("hadoop-3.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_2, HdfsVersion.find("hadoop3.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_3, HdfsVersion.find("3.3.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_3, HdfsVersion.find("3.3.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_3, HdfsVersion.find("hadoop-3.3"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_3, HdfsVersion.find("hadoop-3.3.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_3_3, HdfsVersion.find("hadoop3.3"));
  }
}
