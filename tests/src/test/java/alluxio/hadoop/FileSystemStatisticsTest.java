/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.hadoop;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import alluxio.LocalAlluxioClusterResource;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;

/**
 * Integration tests for statistics in TFS.
 */
public class FileSystemStatisticsTest {

  private static final int BLOCK_SIZE = 128;
  private static final int FILE_LEN = BLOCK_SIZE * 2 + 1;
  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(10000, BLOCK_SIZE);
  private static org.apache.hadoop.fs.FileSystem.Statistics sStatistics;
  private static org.apache.hadoop.fs.FileSystem sTFS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());

    alluxio.client.file.FileSystem alluxioFS = sLocalAlluxioClusterResource.get().getClient();
    FileSystemTestUtils.createByteFile(alluxioFS, "/testFile-read", WriteType.CACHE_THROUGH,
        FILE_LEN);

    URI uri = URI.create(sLocalAlluxioClusterResource.get().getMasterUri());
    sTFS = org.apache.hadoop.fs.FileSystem.get(uri, conf);
    sStatistics = org.apache.hadoop.fs.FileSystem.getStatistics(uri.getScheme(), sTFS.getClass());
  }

  /**
   * Test the number of bytes read.
   */
  @Test
  public void bytesReadStatisticsTest() throws Exception {
    long originStat = sStatistics.getBytesRead();
    InputStream is = sTFS.open(new Path("/testFile-read"));
    while (is.read() != -1) {
    }
    is.close();
    Assert.assertEquals(originStat + FILE_LEN, sStatistics.getBytesRead());
  }

  /**
   * Test the number of bytes written.
   */
  @Test
  public void bytesWrittenStatisticsTest() throws Exception {
    long originStat = sStatistics.getBytesWritten();
    OutputStream os = sTFS.create(new Path("/testFile-write"));
    for (int i = 0; i < FILE_LEN; i ++) {
      os.write(1);
    }
    os.close();
    Assert.assertEquals(originStat + FILE_LEN, sStatistics.getBytesWritten());
  }

  /**
   * Test the number of read/write operations.
   */
  @Test
  public void readWriteOperationsStatisticsTest() throws Exception {
    int exceptedReadOps = sStatistics.getReadOps();
    int exceptedWriteOps = sStatistics.getWriteOps();

    // Call all the overridden methods and check the statistics.
    sTFS.create(new Path("/testFile-create")).close();
    exceptedWriteOps ++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.delete(new Path("/testFile-create"), true);
    exceptedWriteOps ++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.getDefaultBlockSize();
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.getDefaultReplication();
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    FileStatus fStatus = sTFS.getFileStatus(new Path("/testFile-read"));
    exceptedReadOps ++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.getFileBlockLocations(fStatus, 0, FILE_LEN);
    exceptedReadOps ++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.getUri();
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.getWorkingDirectory();
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.listStatus(new Path("/"));
    exceptedReadOps ++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.mkdirs(new Path("/testDir"));
    exceptedWriteOps ++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.open(new Path("/testFile-read")).close();
    exceptedReadOps ++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.rename(new Path("/testDir"), new Path("/testDir-rename"));
    exceptedWriteOps ++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.setWorkingDirectory(new Path("/testDir-rename"));
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());
  }
}
