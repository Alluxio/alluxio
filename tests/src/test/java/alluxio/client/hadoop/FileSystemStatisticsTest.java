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

package alluxio.client.hadoop;

import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.WritePType;
import alluxio.hadoop.FileSystem;
import alluxio.hadoop.HadoopConfigurationUtils;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Integration tests for statistics in TFS.
 */
public class FileSystemStatisticsTest extends BaseIntegrationTest {

  private static final int BLOCK_SIZE = 128;
  private static final int FILE_LEN = BLOCK_SIZE * 2 + 1;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE)
          .build();
  private static org.apache.hadoop.fs.FileSystem.Statistics sStatistics;
  private static org.apache.hadoop.fs.FileSystem sTFS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());

    alluxio.client.file.FileSystem alluxioFS = sLocalAlluxioClusterResource.get().getClient();
    FileSystemTestUtils.createByteFile(alluxioFS, "/testFile-read", WritePType.CACHE_THROUGH,
        FILE_LEN);

    URI uri = URI.create(sLocalAlluxioClusterResource.get().getMasterURI());
    sTFS = org.apache.hadoop.fs.FileSystem.get(uri, HadoopConfigurationUtils
        .mergeAlluxioConfiguration(conf, ServerConfiguration.global()));
    sStatistics = org.apache.hadoop.fs.FileSystem.getStatistics(uri.getScheme(), sTFS.getClass());
  }

  /**
   * Test the number of bytes read.
   */
  @Test
  public void bytesReadStatistics() throws Exception {
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
  public void bytesWrittenStatistics() throws Exception {
    long originStat = sStatistics.getBytesWritten();
    OutputStream os = sTFS.create(new Path("/testFile-write"));
    for (int i = 0; i < FILE_LEN; i++) {
      os.write(1);
    }
    os.close();
    Assert.assertEquals(originStat + FILE_LEN, sStatistics.getBytesWritten());
  }

  /**
   * Test the number of read/write operations.
   */
  @Test
  public void readWriteOperationsStatistics() throws Exception {
    int exceptedReadOps = sStatistics.getReadOps();
    int exceptedWriteOps = sStatistics.getWriteOps();

    // Call all the overridden methods and check the statistics.
    sTFS.create(new Path("/testFile-create")).close();
    exceptedWriteOps++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.delete(new Path("/testFile-create"), true);
    exceptedWriteOps++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
    // FileSystem.getDefaultBlockSize(new Path("/testFile-create")) will be the new one.
    sTFS.getDefaultBlockSize();
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
    // FileSystem.geDefaultReplication(new Path("/testFile-create")) will be the new one.
    sTFS.getDefaultReplication();
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    FileStatus fStatus = sTFS.getFileStatus(new Path("/testFile-read"));
    exceptedReadOps++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.getFileBlockLocations(fStatus, 0, FILE_LEN);
    exceptedReadOps++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.getUri();
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.getWorkingDirectory();
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.listStatus(new Path("/"));
    exceptedReadOps++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.mkdirs(new Path("/testDir"));
    exceptedWriteOps++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.open(new Path("/testFile-read")).close();
    exceptedReadOps++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.rename(new Path("/testDir"), new Path("/testDir-rename"));
    exceptedWriteOps++;
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());

    sTFS.setWorkingDirectory(new Path("/testDir-rename"));
    Assert.assertEquals(exceptedReadOps, sStatistics.getReadOps());
    Assert.assertEquals(exceptedWriteOps, sStatistics.getWriteOps());
  }
}
