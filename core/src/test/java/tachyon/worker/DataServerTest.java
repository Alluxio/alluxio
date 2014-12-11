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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for tachyon.DataServer.
 */
@RunWith(Parameterized.class)
public class DataServerTest {
  private static final int WORKER_CAPACITY_BYTES = 1000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // creates a new instance of DataServerTest for each network type
    List<Object[]> list = new ArrayList<Object[]>();
    for (final NetworkType type : NetworkType.values()) {
      list.add(new Object[] {type});
    }
    return list;
  }

  private final NetworkType mType;
  private LocalTachyonCluster mLocalTachyonCluster = null;

  private TachyonFS mTFS = null;

  public DataServerTest(NetworkType type) {
    mType = type;
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.worker.network.type");
  }

  /**
   * Asserts that the message back matches the block response protocols for the error case.
   */
  private void assertError(final DataClient.GetBlock msg, final long blockId) {
    assertValid(msg, 0, -1 * blockId, 0, 0);
  }

  /**
   * Asserts that the message back matches the block response protocols.
   */
  private void assertValid(final DataClient.GetBlock msg, final ByteBuffer expectedData,
      final long blockId, final long offset, final long length) {
    Assert.assertEquals(expectedData, ByteBuffer.wrap(msg.getData()));
    Assert.assertEquals(blockId, msg.getBlockId());
    Assert.assertEquals(offset, msg.getOffset());
    Assert.assertEquals(length, msg.getLength());
  }

  /**
   * Asserts that the message back matches the block response protocols.
   */
  private void assertValid(final DataClient.GetBlock msg, final int expectedSize,
      final long blockId, final long offset, final long length) {
    assertValid(msg, TestUtils.getIncreasingByteBuffer(expectedSize), blockId, offset, length);
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    System.setProperty("tachyon.worker.network.type", mType.toString());
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mTFS = mLocalTachyonCluster.getClient();
  }

  @Test(expected = IOException.class)
  public void lengthTooSmall() throws IOException {
    final int length = 20;
    int fileId = TestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataClient.GetBlock recvMsg = request(block, 0, length * -2);
    assertError(recvMsg, block.blockId);
  }

  @Test
  public void multiReadTest() throws IOException {
    final int length = 20;
    int fileId = TestUtils.createByteFile(mTFS, "/multiReadTest", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    for (int i = 0; i < 10; i++) {
      DataClient.GetBlock recvMsg = request(block);
      assertValid(recvMsg, length, block.getBlockId(), 0, length);
    }
  }

  @Test(expected = IOException.class)
  public void negativeOffset() throws IOException {
    final int length = 10;
    int fileId = TestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataClient.GetBlock recvMsg = request(block, length * -2, 1);
    assertError(recvMsg, block.blockId);
  }

  @Test
  public void readMultiFiles() throws IOException {
    final int length = WORKER_CAPACITY_BYTES / 2 + 1;
    int fileId1 = TestUtils.createByteFile(mTFS, "/readFile1", WriteType.MUST_CACHE, length);
    ClientBlockInfo block1 = mTFS.getFileBlocks(fileId1).get(0);
    DataClient.GetBlock recvMsg1 = request(block1);
    assertValid(recvMsg1, length, block1.getBlockId(), 0, length);

    int fileId2 = TestUtils.createByteFile(mTFS, "/readFile2", WriteType.MUST_CACHE, length);
    ClientBlockInfo block2 = mTFS.getFileBlocks(fileId2).get(0);
    DataClient.GetBlock recvMsg2 = request(block2);
    assertValid(recvMsg2, length, block2.getBlockId(), 0, length);

    CommonUtils.sleepMs(null, WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS);
    ClientFileInfo fileInfo = mTFS.getFileStatus(-1, new TachyonURI("/readFile1"));
    Assert.assertEquals(0, fileInfo.inMemoryPercentage);
  }

  @Test
  public void readPartialTest1() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    final int offset = 0;
    final int length = 6;
    DataClient.GetBlock recvMsg = request(block, offset, length);
    assertValid(recvMsg, length, block.getBlockId(), offset, length);
  }

  @Test
  public void readPartialTest2() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    final int offset = 2;
    final int length = 6;
    DataClient.GetBlock recvMsg = request(block, offset, length);
    assertValid(recvMsg, TestUtils.getIncreasingByteBuffer(offset, length), block.getBlockId(),
        offset, length);
  }

  @Test
  public void readTest() throws InvalidPathException, FileAlreadyExistException, IOException {
    final int length = 10;
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataClient.GetBlock recvMsg = request(block);
    assertValid(recvMsg, length, block.getBlockId(), 0, length);
  }

  @Test(expected = IOException.class)
  public void readTooLarge() throws IOException {
    final int length = 20;
    int fileId = TestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataClient.GetBlock recvMsg = request(block, 0, length * 2);
    assertError(recvMsg, block.blockId);
  }

  /**
   * Requests a block from the server. This call will read the full block.
   */
  private DataClient.GetBlock request(final ClientBlockInfo block) throws IOException {
    return request(block, 0, -1);
  }

  /**
   * Create a new socket to the data port and send a block request. The returned value is the
   * response from the server.
   */
  private DataClient.GetBlock request(final ClientBlockInfo block, final long offset,
      final long length) throws IOException {
    NetAddress location = block.getLocations().get(0);
    return new DataClient(location.mHost, location.mSecondaryPort).getBlock(block.blockId, offset,
        length);
  }

  @Test(expected = IOException.class)
  public void tooLargeOffset() throws IOException {
    final int length = 10;
    int fileId = TestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataClient.GetBlock recvMsg = request(block, length * 2, 1);
    assertError(recvMsg, block.blockId);
  }
}
