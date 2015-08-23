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

package tachyon.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;

/**
 * Integration tests for TFS getFileBlockLocations.
 */
public class TFSBlockLocationIntegrationTest {

  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_LEN = BLOCK_SIZE * 3;
  private static LocalTachyonCluster sLocalTachyonCluster;
  private static FileSystem sTFS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.tachyon.impl", TFS.class.getName());

    // Start local Tachyon cluster
    sLocalTachyonCluster = new LocalTachyonCluster(100000000, 100000, BLOCK_SIZE);
    sLocalTachyonCluster.start();

    TachyonFS tachyonFS = sLocalTachyonCluster.getClient();
    TachyonFSTestUtils.createByteFile(tachyonFS, "/testFile1", WriteType.CACHE_THROUGH, FILE_LEN);
    tachyonFS.close();

    URI uri = URI.create(sLocalTachyonCluster.getMasterUri());
    sTFS = FileSystem.get(uri, conf);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
  }

  /**
   * Test <code>BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)</code>.
   * Test the different situations of different start and len.
   */
  @Test
  public void basicBlockLocationTest() throws Exception {
    long start = 0;
    long len = 0;
    FileStatus fStatus = sTFS.getFileStatus(new Path("/testFile1"));

    //block0.offset = start < start+len < block1.offset
    start = 0;
    len = BLOCK_SIZE - 1;
    Assert.assertEquals(1, sTFS.getFileBlockLocations(fStatus, start, len).length);

    //block0.offset < start < start+len < block1.offset
    start = 1;
    len = BLOCK_SIZE - 2;
    Assert.assertEquals(1, sTFS.getFileBlockLocations(fStatus, start, len).length);

    //block0.offset < start = start+len < block1.offset
    start = 1;
    len = 0;
    Assert.assertEquals(1, sTFS.getFileBlockLocations(fStatus, start, len).length);

    //block0.offset = start < start+len = block1.offset
    start = 0;
    len = BLOCK_SIZE;
    Assert.assertEquals(2, sTFS.getFileBlockLocations(fStatus, start, len).length);

    //block0.offset = start < block1.offset < start+len < block2.offset
    start = 0;
    len = BLOCK_SIZE + 1;
    Assert.assertEquals(2, sTFS.getFileBlockLocations(fStatus, start, len).length);

    //block0.offset < start < block1.offset < start+len < block2.offset
    start = 1;
    len = BLOCK_SIZE;
    Assert.assertEquals(2, sTFS.getFileBlockLocations(fStatus, start, len).length);

    //block0.offset = start < start+len = block2.offset
    start = 0;
    len = BLOCK_SIZE * 2;
    Assert.assertEquals(3, sTFS.getFileBlockLocations(fStatus, start, len).length);

    //block0.offset = start < start+len = file.len
    start = 0;
    len = FILE_LEN;
    Assert.assertEquals(3, sTFS.getFileBlockLocations(fStatus, start, len).length);

    //file.len < start < start+len
    start = FILE_LEN + 1;
    len = 1;
    Assert.assertEquals(0, sTFS.getFileBlockLocations(fStatus, start, len).length);
  }

}
