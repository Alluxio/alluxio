/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.IntegrationTestConstants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.RemoteBlockReader;
import alluxio.client.WriteType;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.network.protocol.RPCResponse;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for {@link DataServer}.
 */
@RunWith(Parameterized.class)
public class DataServerIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = Constants.MB;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // Creates a new instance of DataServerIntegrationTest for different combinations of parameters.
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.MAPPED_TRANSFER, IntegrationTestConstants.NETTY_BLOCK_READER});
    list.add(new Object[] {IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.FILE_CHANNEL_TRANSFER,
        IntegrationTestConstants.NETTY_BLOCK_READER});
    return list;
  }

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource;
  private FileSystem mFileSystem = null;
  private Configuration mWorkerConfiguration;
  private BlockMasterClient mBlockMasterClient;
  private BlockWorkerClient mBlockWorkerClient;

  public DataServerIntegrationTest(String className, String nettyTransferType, String blockReader) {
    mLocalAlluxioClusterResource = new LocalAlluxioClusterResource(WORKER_CAPACITY_BYTES,
        Constants.MB, Constants.WORKER_DATA_SERVER, className,
        Constants.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, nettyTransferType,
        Constants.USER_FILE_BUFFER_BYTES, String.valueOf(100), Constants.USER_BLOCK_REMOTE_READER,
        blockReader);
  }

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

  @Before
  public final void before() throws Exception {
    mWorkerConfiguration = mLocalAlluxioClusterResource.get().getWorkerConf();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();

    mBlockWorkerClient = BlockStoreContext.INSTANCE.acquireLocalWorkerClient();
    mBlockMasterClient = new BlockMasterClient(
        new InetSocketAddress(mLocalAlluxioClusterResource.get().getMasterHostname(),
            mLocalAlluxioClusterResource.get().getMasterPort()),
        mWorkerConfiguration);
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
  public void lengthTooSmall() throws IOException, AlluxioException {
    final int length = 20;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));
    DataServerMessage recvMsg = request(block, 0, length * -2);
    assertError(recvMsg, block.getBlockId());
  }

  @Test
  public void multiReadTest() throws IOException, AlluxioException {
    final int length = 20;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));
    for (int i = 0; i < 10; i++) {
      DataServerMessage recvMsg = request(block);
      assertValid(recvMsg, length, block.getBlockId(), 0, length);
    }
  }

  @Test
  public void negativeOffset() throws IOException, AlluxioException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));
    DataServerMessage recvMsg = request(block, length * -2, 1);
    assertError(recvMsg, block.getBlockId());
  }

  @Test
  public void readMultiFiles() throws Exception {
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    final int length = WORKER_CAPACITY_BYTES / 2 + 1;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file1", WriteType.MUST_CACHE, length);
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    BlockInfo block1 = getFirstBlockInfo(new AlluxioURI("/file1"));
    DataServerMessage recvMsg1 = request(block1);
    assertValid(recvMsg1, length, block1.getBlockId(), 0, length);

    FileSystemTestUtils.createByteFile(mFileSystem, "/file2", WriteType.MUST_CACHE, length);
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    BlockInfo block2 = getFirstBlockInfo(new AlluxioURI("/file2"));
    DataServerMessage recvMsg2 = request(block2);
    assertValid(recvMsg2, length, block2.getBlockId(), 0, length);

    Assert.assertEquals(0, mFileSystem.getStatus(new AlluxioURI("/file1")).getInMemoryPercentage());
  }

  @Test
  public void readPartialTest1() throws AlluxioException, IOException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, 10);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));
    final int offset = 0;
    final int length = 6;
    DataServerMessage recvMsg = request(block, offset, length);
    assertValid(recvMsg, length, block.getBlockId(), offset, length);
  }

  @Test
  public void readPartialTest2() throws AlluxioException, IOException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, 10);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));
    final int offset = 2;
    final int length = 6;
    DataServerMessage recvMsg = request(block, offset, length);
    assertValid(recvMsg, BufferUtils.getIncreasingByteBuffer(offset, length), block.getBlockId(),
        offset, length);
  }

  @Test
  public void readTest() throws IOException, AlluxioException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));
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
  public void readThroughClientTest() throws IOException, AlluxioException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));

    RemoteBlockReader client =
        RemoteBlockReader.Factory.create(mWorkerConfiguration);
    ByteBuffer result = readRemotely(client, block, length);

    Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(length), result);
  }

  // TODO(calvin): Make this work with the new BlockReader.
  // @Test
  public void readThroughClientNonExistentTest() throws IOException, AlluxioException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));

    // Get the maximum block id, for use in determining a non-existent block id.
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/file"));
    long maxBlockId = block.getBlockId();
    for (long blockId : status.getBlockIds()) {
      if (blockId > maxBlockId) {
        maxBlockId = blockId;
      }
    }

    RemoteBlockReader client =
        RemoteBlockReader.Factory.create(mWorkerConfiguration);
    block.setBlockId(maxBlockId + 1);
    ByteBuffer result = readRemotely(client, block, length);

    Assert.assertNull(result);
  }

  @Test
  public void readTooLarge() throws IOException, AlluxioException {
    final int length = 20;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));
    DataServerMessage recvMsg = request(block, 0, length * 2);
    assertError(recvMsg, block.getBlockId());
  }

  @Test
  public void tooLargeOffset() throws IOException, AlluxioException {
    final int length = 10;
    FileSystemTestUtils.createByteFile(mFileSystem, "/file", WriteType.MUST_CACHE, length);
    BlockInfo block = getFirstBlockInfo(new AlluxioURI("/file"));
    DataServerMessage recvMsg = request(block, length * 2, 1);
    assertError(recvMsg, block.getBlockId());
  }

  /**
   * Requests a block from the server. This call will read the full block.
   */
  private DataServerMessage request(final BlockInfo block) throws IOException, AlluxioException {
    return request(block, 0, -1);
  }

  /**
   * Creates a new socket to the data port and send a block request. The returned value is the
   * response from the server.
   */
  private DataServerMessage request(final BlockInfo block, final long offset, final long length)
      throws IOException, AlluxioException {
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
   * Returns the MasterBlockInfo of the first block of the file.
   *
   * @param uri the uri of the file to get the first MasterBlockInfo for
   * @return the MasterBlockInfo of the first block in the file
   * @throws IOException if the block does not exist
   * @throws AlluxioException
   */
  private BlockInfo getFirstBlockInfo(AlluxioURI uri)
      throws IOException, AlluxioException {
    URIStatus status = mFileSystem.getStatus(uri);
    return mBlockMasterClient.getBlockInfo(status.getBlockIds().get(0));
  }
}
