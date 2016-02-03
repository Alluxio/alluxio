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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import tachyon.Constants;
import tachyon.IntegrationTestConstants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.RemoteBlockReader;
import tachyon.client.FileSystemTestUtils;
import tachyon.client.WriteType;
import tachyon.client.block.BlockMasterClient;
import tachyon.client.block.BlockStoreContext;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.block.BlockWorkerClient;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.exception.TachyonException;
import tachyon.network.protocol.RPCResponse;
import tachyon.thrift.BlockInfo;
import tachyon.util.CommonUtils;
import tachyon.util.io.BufferUtils;

/**
 * Integration tests for {@link DataServer}.
 */
@RunWith(Parameterized.class)
public class DataServerIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // Creates a new instance of DataServerIntegrationTest for different combinations of parameters.
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.MAPPED_TRANSFER, IntegrationTestConstants.NETTY_BLOCK_READER});
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.FILE_CHANNEL_TRANSFER,
        IntegrationTestConstants.NETTY_BLOCK_READER});
    // The transfer type is not applicable to the NIODataServer.
    list.add(new Object[] {IntegrationTestConstants.NIO_DATA_SERVER,
        IntegrationTestConstants.UNUSED_TRANSFER, IntegrationTestConstants.NETTY_BLOCK_READER});
    return list;
  }

  private final String mDataServerClass;
  private final String mNettyTransferType;
  private final String mBlockReader;

  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource;
  private FileSystem mFileSystem = null;
  private TachyonConf mWorkerTachyonConf;
  private BlockMasterClient mBlockMasterClient;
  private BlockWorkerClient mBlockWorkerClient;

  public DataServerIntegrationTest(String className, String nettyTransferType, String blockReader) {
    mDataServerClass = className;
    mNettyTransferType = nettyTransferType;
    mBlockReader = blockReader;

    mLocalTachyonClusterResource = new LocalTachyonClusterResource(WORKER_CAPACITY_BYTES,
        Constants.MB, Constants.WORKER_DATA_SERVER, mDataServerClass,
        Constants.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, mNettyTransferType,
        Constants.USER_FILE_BUFFER_BYTES, String.valueOf(100), Constants.USER_BLOCK_REMOTE_READER,
        mBlockReader);
  }

  @Before
  public final void before() throws Exception {
    mWorkerTachyonConf = mLocalTachyonClusterResource.get().getWorkerTachyonConf();
    mFileSystem = mLocalTachyonClusterResource.get().getClient();

    mBlockWorkerClient = BlockStoreContext.INSTANCE.acquireWorkerClient();
    mBlockMasterClient = new BlockMasterClient(
        new InetSocketAddress(mLocalTachyonClusterResource.get().getMasterHostname(),
            mLocalTachyonClusterResource.get().getMasterPort()),
        mWorkerTachyonConf);
  }

  @After
  public final void after() throws Exception {
    mBlockMasterClient.close();
  }

  /**
   * Asserts that the message back matches the block response protocols for the error case.
   */
  private void assertError(final DataServerMessage msg, final long blockId) {
    Assert.assertEquals(blockId, msg.getBlockId());
    Assert.assertEquals(0, msg.getLength());
    Assert.assertNotEquals(msg.getStatus().getId(), RPCResponse.Status.SUCCESS.getId());
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
  private void assertValid(final DataServerMessage msg, final int expectedSize, final long blockId,
      final long offset, final long length) {
    assertValid(msg, BufferUtils.getIncreasingByteBuffer(expectedSize), blockId, offset, length);
  }

  @Test
  public void lengthTooSmall() throws IOException, TachyonException {
    final int length = 20;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));
    DataServerMessage recvMsg = request(block, 0, length * -2);
    assertError(recvMsg, block.getBlockId());
  }

  @Test
  public void multiReadTest() throws IOException, TachyonException {
    final int length = 20;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));
    for (int i = 0; i < 10; i ++) {
      DataServerMessage recvMsg = request(block);
      assertValid(recvMsg, length, block.getBlockId(), 0, length);
    }
  }

  @Test
  public void negativeOffset() throws IOException, TachyonException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));
    DataServerMessage recvMsg = request(block, length * -2, 1);
    assertError(recvMsg, block.getBlockId());
  }

  @Test
  public void readMultiFiles() throws IOException, TachyonException {
    final int length = WORKER_CAPACITY_BYTES / 2 + 1;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file1", WriteType.MUST_CACHE, length);
    BlockInfo block1 = getFirstBlockInfo(new TachyonURI("/file1"));
    DataServerMessage recvMsg1 = request(block1);
    assertValid(recvMsg1, length, block1.getBlockId(), 0, length);

    FileSystemTestUtils.createByteFile(mFileSystem, "/file2", WriteType.MUST_CACHE, length);
    BlockInfo block2 = getFirstBlockInfo(new TachyonURI("/file2"));
    DataServerMessage recvMsg2 = request(block2);
    assertValid(recvMsg2, length, block2.getBlockId(), 0, length);

    CommonUtils
        .sleepMs(mWorkerTachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2
            + 10);

    Assert.assertEquals(0, mFileSystem.getStatus(new TachyonURI("/file1")).getInMemoryPercentage());
  }

  @Test
  public void readPartialTest1() throws TachyonException, IOException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, 10);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));
    final int offset = 0;
    final int length = 6;
    DataServerMessage recvMsg = request(block, offset, length);
    assertValid(recvMsg, length, block.getBlockId(), offset, length);
  }

  @Test
  public void readPartialTest2() throws TachyonException, IOException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, 10);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));
    final int offset = 2;
    final int length = 6;
    DataServerMessage recvMsg = request(block, offset, length);
    assertValid(recvMsg, BufferUtils.getIncreasingByteBuffer(offset, length), block.getBlockId(),
        offset, length);
  }

  @Test
  public void readTest() throws IOException, TachyonException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));
    DataServerMessage recvMsg = request(block);
    assertValid(recvMsg, length, block.getBlockId(), 0, length);
  }

  private ByteBuffer readRemotely(RemoteBlockReader client, BlockInfo block, int length)
      throws IOException, ConnectionFailedException {
    long lockId = mBlockWorkerClient.lockBlock(block.getBlockId()).getLockId();
    try {
      return client.readRemoteBlock(
          new InetSocketAddress(block.getLocations().get(0).getWorkerAddress().getHost(),
              block.getLocations().get(0).getWorkerAddress().getDataPort()),
          block.getBlockId(), 0, length, lockId, mBlockWorkerClient.getSessionId());
    } finally {
      mBlockWorkerClient.unlockBlock(block.getBlockId());
    }
  }

  @Test
  public void readThroughClientTest() throws IOException, TachyonException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));

    RemoteBlockReader client =
        RemoteBlockReader.Factory.create(mWorkerTachyonConf);
    ByteBuffer result = readRemotely(client, block, length);

    Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(length), result);
  }

  // TODO(calvin): Make this work with the new BlockReader.
  // @Test
  public void readThroughClientNonExistentTest() throws IOException, TachyonException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));

    // Get the maximum block id, for use in determining a non-existent block id.
    URIStatus status = mFileSystem.getStatus(new TachyonURI("/file"));
    long maxBlockId = block.getBlockId();
    for (long blockId : status.getBlockIds()) {
      if (blockId > maxBlockId) {
        maxBlockId = blockId;
      }
    }

    RemoteBlockReader client =
        RemoteBlockReader.Factory.create(mWorkerTachyonConf);
    block.setBlockId(maxBlockId + 1);
    ByteBuffer result = readRemotely(client, block, length);

    Assert.assertNull(result);
  }

  @Test
  public void readTooLarge() throws IOException, TachyonException {
    final int length = 20;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));
    DataServerMessage recvMsg = request(block, 0, length * 2);
    assertError(recvMsg, block.getBlockId());
  }

  @Test
  public void tooLargeOffset() throws IOException, TachyonException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new TachyonURI("/file"));
    DataServerMessage recvMsg = request(block, length * 2, 1);
    assertError(recvMsg, block.getBlockId());
  }

  /**
   * Requests a block from the server. This call will read the full block.
   */
  private DataServerMessage request(final BlockInfo block) throws IOException, TachyonException {
    return request(block, 0, -1);
  }

  /**
   * Create a new socket to the data port and send a block request. The returned value is the
   * response from the server.
   */
  private DataServerMessage request(final BlockInfo block, final long offset, final long length)
      throws IOException, TachyonException {
    long lockId = mBlockWorkerClient.lockBlock(block.getBlockId()).getLockId();

    SocketChannel socketChannel = null;

    try {
      DataServerMessage sendMsg =
          DataServerMessage.createBlockRequestMessage(block.getBlockId(), offset, length, lockId,
              mBlockWorkerClient.getSessionId());
      socketChannel = SocketChannel
        .open(new InetSocketAddress(block.getLocations().get(0).getWorkerAddress().getHost(),
            block.getLocations().get(0).getWorkerAddress().getDataPort()));

      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }
      DataServerMessage recvMsg =
          DataServerMessage.createBlockResponseMessage(false, block.getBlockId(), offset, length,
                  null);
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          break;
        }
      }
      return recvMsg;
    } finally {
      mBlockWorkerClient.unlockBlock(block.getBlockId());
      if (socketChannel != null) {
        socketChannel.close();
      }
    }
  }

  /**
   * Returns the MasterBlockInfo of the first block of the file
   *
   * @param uri the uri of the file to get the first MasterBlockInfo for
   * @return the MasterBlockInfo of the first block in the file
   * @throws IOException if the block does not exist
   * @throws TachyonException
   */
  private BlockInfo getFirstBlockInfo(TachyonURI uri)
      throws IOException, TachyonException {
    URIStatus status = mFileSystem.getStatus(uri);
    return mBlockMasterClient.getBlockInfo(status.getBlockIds().get(0));
  }
}
