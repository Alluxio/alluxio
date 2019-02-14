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

import java.net.URI;

/**
 * Integration tests for {@link FileSystem#getFileBlockLocations(FileStatus, long, long)}.
 */
public class FileSystemBlockLocationIntegrationTest extends BaseIntegrationTest {

  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_LEN = BLOCK_SIZE * 3;
  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private static org.apache.hadoop.fs.FileSystem sTFS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());

    alluxio.client.file.FileSystem alluxioFS = sLocalAlluxioClusterResource.get().getClient();
    FileSystemTestUtils.createByteFile(alluxioFS, "/testFile1", WritePType.CACHE_THROUGH,
        FILE_LEN);

    URI uri = URI.create(sLocalAlluxioClusterResource.get().getMasterURI());
    sTFS = org.apache.hadoop.fs.FileSystem.get(uri, HadoopConfigurationUtils
        .mergeAlluxioConfiguration(conf, ServerConfiguration.global()));
  }

  /**
   * Test {@code BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)}.
   * Test the different situations of different start and len.
   */
  @Test
  public void basicBlockLocation() throws Exception {
    FileStatus fStatus = sTFS.getFileStatus(new Path("/testFile1"));

    // block0.offset = start < start+len < block1.offset
    long start = 0;
    long len = BLOCK_SIZE - 1;
    Assert.assertEquals(1, sTFS.getFileBlockLocations(fStatus, start, len).length);

    // block0.offset < start < start+len < block1.offset
    start = 1;
    len = BLOCK_SIZE - 2;
    Assert.assertEquals(1, sTFS.getFileBlockLocations(fStatus, start, len).length);

    // block0.offset < start = start+len < block1.offset
    start = 1;
    len = 0;
    Assert.assertEquals(1, sTFS.getFileBlockLocations(fStatus, start, len).length);

    // block0.offset = start < start+len = block1.offset
    start = 0;
    len = BLOCK_SIZE;
    Assert.assertEquals(2, sTFS.getFileBlockLocations(fStatus, start, len).length);

    // block0.offset = start < block1.offset < start+len < block2.offset
    start = 0;
    len = BLOCK_SIZE + 1;
    Assert.assertEquals(2, sTFS.getFileBlockLocations(fStatus, start, len).length);

    // block0.offset < start < block1.offset < start+len < block2.offset
    start = 1;
    len = BLOCK_SIZE;
    Assert.assertEquals(2, sTFS.getFileBlockLocations(fStatus, start, len).length);

    // block0.offset = start < start+len = block2.offset
    start = 0;
    len = BLOCK_SIZE * 2;
    Assert.assertEquals(3, sTFS.getFileBlockLocations(fStatus, start, len).length);

    // block0.offset = start < start+len = file.len
    start = 0;
    len = FILE_LEN;
    Assert.assertEquals(3, sTFS.getFileBlockLocations(fStatus, start, len).length);

    // file.len < start < start+len
    start = FILE_LEN + 1;
    len = 1;
    Assert.assertEquals(0, sTFS.getFileBlockLocations(fStatus, start, len).length);
  }
}
