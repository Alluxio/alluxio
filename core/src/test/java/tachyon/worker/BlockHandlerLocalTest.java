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
package tachyon.worker;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;

/**
 * Unit tests for <code>tachyon.client.BlockHandlerLocal</code>.
 */
public class BlockHandlerLocalTest {
  private static LocalTachyonCluster mLocalTachyonCluster = null;
  private static TachyonFS mTfs = null;

  @AfterClass
  public static final void afterClass() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @BeforeClass
  public static final void beforeClass() throws IOException {
    mLocalTachyonCluster = new LocalTachyonCluster(10000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  private void bufferReadTest(boolean direct) throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, TestUtils.uniqPath(), WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String filename = file.getLocalFilename(0);
    BlockHandler handler = BlockHandler.Factory.get(filename);
    ByteBuffer buf;
    if (direct) {
      buf = ByteBuffer.allocateDirect(100);
    } else {
      buf = ByteBuffer.allocate(100);
    }
    try {
      long pos = handler.position();
      Assert.assertEquals(0, pos);
      int len = handler.read(buf);
      buf.flip();
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(100), buf);
      Assert.assertEquals(100, handler.position());
      Assert.assertEquals(100, len);
      handler.position(10);
      Assert.assertEquals(10, handler.position());
    } finally {
      handler.close();
    }
  }

  private void bufferWriteTest(boolean direct) throws IOException {
    ByteBuffer buf;
    if (direct) {
      buf = ByteBuffer.allocateDirect(100);
    } else {
      buf = ByteBuffer.allocate(100);
    }
    buf.put(TestUtils.getIncreasingByteArray(100));
    buf.flip();

    int fileId = mTfs.createFile(new TachyonURI(TestUtils.uniqPath()));
    long blockId = mTfs.getBlockId(fileId, 0);
    String filename = mTfs.getLocalBlockTemporaryPath(blockId, 100);
    BlockHandler handler = BlockHandler.Factory.get(filename);
    try {
      handler.write(buf);
    } finally {
      handler.close();
    }
    mTfs.cacheBlock(blockId);
    TachyonFile file = mTfs.getFile(fileId);
    long fileLen = file.length();
    Assert.assertEquals(100, fileLen);
  }

  @Test
  public void readTest() throws IOException {
    bufferReadTest(false);
    bufferReadTest(true);
  }

  @Test
  public void writeTest() throws IOException {
    bufferWriteTest(false);
    bufferWriteTest(true);
  }
}
