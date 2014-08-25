/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.CommonUtils;

public class BlockHandlerLocalTest {

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  @Test
  public void readTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, "/root/testFile", WriteType.MUST_CACHE, 100);
    long blockId = mTfs.getBlockId(fileId, 0);
    String filename = mTfs.getLocalFilename(blockId);
    BlockHandler handler = BlockHandler.get(filename);
    ByteBuffer buf = handler.read(0, 100);
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(100), buf);
    buf = handler.read(0, -1);
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(100), buf);
    handler.close();
    return;
  }

  @Test
  public void writeTest() throws IOException {
    int fileId = mTfs.createFile("/root/testFile");
    long blockId = mTfs.getBlockId(fileId, 0);
    String localFolder = mTfs.createAndGetUserTempFolder().getPath();
    String filename = CommonUtils.concat(localFolder, blockId);
    BlockHandler handler = BlockHandler.get(filename);
    byte[] buf = TestUtils.getIncreasingByteArray(100);
    handler.append(0, ByteBuffer.wrap(buf));
    handler.close();
    mTfs.cacheBlock(blockId);
    long fileLen = mTfs.getFileLength(fileId);
    Assert.assertEquals(100, fileLen);
    return;
  }
}
