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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.RemoteBlockReader;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;

/**
 * Integration tests for tachyon.worker.DataServer.
 */
@RunWith(Parameterized.class)
public class DataServerIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 1000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  private static final String UNUSED_TRANSFER_TYPE = "UNUSED";

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // Creates a new instance of DataServerTest for each network type and transfer type.
    // The transfer type is only applicable to the netty DataServer.
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] { "tachyon.worker.netty.NettyDataServer", "MAPPED" });
    list.add(new Object[] { "tachyon.worker.netty.NettyDataServer", "TRANSFER" });
    list.add(new Object[] { "tachyon.worker.nio.NIODataServer", UNUSED_TRANSFER_TYPE });
    return list;
  }

  private final String mDataServerClass;
  private final String mNettyTransferType;
  private LocalTachyonCluster mLocalTachyonCluster = null;

  private TachyonFS mTFS = null;

  private TachyonConf mWorkerTachyonConf;

  public DataServerIntegrationTest(String className, String nettyTransferType) {
    mDataServerClass = className;
    mNettyTransferType = nettyTransferType;
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty(Constants.WORKER_DATA_SERVER);
    System.clearProperty(Constants.WORKER_NETTY_FILE_TRANSFER_TYPE);
    System.clearProperty(Constants.USER_REMOTE_BLOCK_READER);
  }

  /**
   * Asserts that the message back matches the block response protocols for the error case.
   */
  private void assertError(final DataServerMessage msg, final long blockId) {
    assertValid(msg, 0, -1 * blockId, 0, 0);
  }

  /**
   * Asserts that the message back matches the block response protocols.
   */
  private void assertValid(final DataServerMessage msg, final ByteBuffer expectedData,
      final long blockId, final long offset, final long length) {
    Assert.assertEquals(expectedData, msg.getReadOnlyData());
    Assert.assertEquals(blockId, msg.getBlockId());
    Assert.assertEquals(offset, msg.getOffset());
    Assert.assertEquals(length, msg.getLength());
  }

  /**
   * Asserts that the message back matches the block response protocols.
   */
  private void assertValid(final DataServerMessage msg, final int expectedSize,
      final long blockId, final long offset, final long length) {
    assertValid(msg, TestUtils.getIncreasingByteBuffer(expectedSize), blockId, offset, length);
  }

  @Before
  public final void before() throws IOException {
    System.setProperty(Constants.WORKER_DATA_SERVER, mDataServerClass);
    System.setProperty(Constants.WORKER_NETTY_FILE_TRANSFER_TYPE, mNettyTransferType);
    System.setProperty(Constants.USER_REMOTE_BLOCK_READER,
        "tachyon.client.netty.NettyRemoteBlockReader");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES,
        Constants.GB);
    mLocalTachyonCluster.start();
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();
    mTFS = mLocalTachyonCluster.getClient();
  }

  @Test
  public void lengthTooSmall() throws IOException {
    final int length = 20;
    int fileId =
        TachyonFSTestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block, 0, length * -2);
    assertError(recvMsg, block.blockId);
  }

  @Test
  public void multiReadTest() throws IOException {
    final int length = 20;
    int fileId =
        TachyonFSTestUtils.createByteFile(mTFS, "/multiReadTest", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    for (int i = 0; i < 10; i ++) {
      DataServerMessage recvMsg = request(block);
      assertValid(recvMsg, length, block.getBlockId(), 0, length);
    }
  }

  @Test
  public void negativeOffset() throws IOException {
    final int length = 10;
    int fileId =
        TachyonFSTestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block, length * -2, 1);
    assertError(recvMsg, block.blockId);
  }

  @Test
  public void readMultiFiles() throws IOException {
    final int length = WORKER_CAPACITY_BYTES / 2 + 1;
    int fileId1 =
        TachyonFSTestUtils.createByteFile(mTFS, "/readFile1", WriteType.MUST_CACHE, length);
    ClientBlockInfo block1 = mTFS.getFileBlocks(fileId1).get(0);
    DataServerMessage recvMsg1 = request(block1);
    assertValid(recvMsg1, length, block1.getBlockId(), 0, length);

    int fileId2 =
        TachyonFSTestUtils.createByteFile(mTFS, "/readFile2", WriteType.MUST_CACHE, length);
    ClientBlockInfo block2 = mTFS.getFileBlocks(fileId2).get(0);
    DataServerMessage recvMsg2 = request(block2);
    assertValid(recvMsg2, length, block2.getBlockId(), 0, length);

    CommonUtils.sleepMs(null,
        TestUtils.getToMasterHeartBeatIntervalMs(mWorkerTachyonConf) * 2 + 10);
    ClientFileInfo fileInfo = mTFS.getFileStatus(-1, new TachyonURI("/readFile1"));
    Assert.assertEquals(0, fileInfo.inMemoryPercentage);
  }

  @Test
  public void readPartialTest1() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TachyonFSTestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    final int offset = 0;
    final int length = 6;
    DataServerMessage recvMsg = request(block, offset, length);
    assertValid(recvMsg, length, block.getBlockId(), offset, length);
  }

  @Test
  public void readPartialTest2() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TachyonFSTestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    final int offset = 2;
    final int length = 6;
    DataServerMessage recvMsg = request(block, offset, length);
    assertValid(recvMsg, TestUtils.getIncreasingByteBuffer(offset, length), block.getBlockId(),
        offset, length);
  }

  @Test
  public void readTest() throws InvalidPathException, FileAlreadyExistException, IOException {
    final int length = 10;
    int fileId = TachyonFSTestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block);
    assertValid(recvMsg, length, block.getBlockId(), 0, length);
  }

  @Test
  public void readThroughClientTest()
      throws InvalidPathException, FileAlreadyExistException, IOException {
    final int length = 10;
    int fileId = TachyonFSTestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);

    RemoteBlockReader client =
        RemoteBlockReader.Factory.createRemoteBlockReader(mWorkerTachyonConf);
    ByteBuffer result = client.readRemoteBlock(block.getLocations().get(0).mHost,
        block.getLocations().get(0).mSecondaryPort, block.getBlockId(), 0, length);

    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(length), result);
  }

  @Test
  public void readTooLarge() throws IOException {
    final int length = 20;
    int fileId =
        TachyonFSTestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block, 0, length * 2);
    assertError(recvMsg, block.blockId);
  }

  /**
   * Requests a block from the server. This call will read the full block.
   */
  private DataServerMessage request(final ClientBlockInfo block) throws IOException {
    return request(block, 0, -1);
  }

  /**
   * Create a new socket to the data port and send a block request. The returned value is the
   * response from the server.
   */
  private DataServerMessage request(final ClientBlockInfo block, final long offset,
      final long length) throws IOException {
    DataServerMessage sendMsg =
        DataServerMessage.createBlockRequestMessage(block.blockId, offset, length);
    SocketChannel socketChannel =
        SocketChannel.open(new InetSocketAddress(block.getLocations().get(0).mHost, block
            .getLocations().get(0).mSecondaryPort));
    try {
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }
      DataServerMessage recvMsg =
          DataServerMessage.createBlockResponseMessage(false, block.blockId, offset, length, null);
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          break;
        }
      }
      return recvMsg;
    } finally {
      socketChannel.close();
    }
  }

  @Test
  public void tooLargeOffset() throws IOException {
    final int length = 10;
    int fileId =
        TachyonFSTestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block, length * 2, 1);
    assertError(recvMsg, block.blockId);
  }
}
