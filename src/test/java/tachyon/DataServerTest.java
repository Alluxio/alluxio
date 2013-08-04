package tachyon;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests for tachyon.DataServer.
 */
public class DataServerTest {
  private final int WORKER_CAPACITY_BYTES = 1000;
  private final int USER_QUOTA_UNIT_BYTES = 100;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTFS = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mTFS = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void readTest() throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.CACHE, 10);
    long blockId = mTFS.getBlockId(fileId, 0);
    DataServerMessage sendMsg = DataServerMessage.createBlockRequestMessage(blockId);
    SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(
        mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mHost,
        mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mPort + 1));
    while (!sendMsg.finishSending()) {
      sendMsg.send(socketChannel);
    }
    DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId);
    while (!recvMsg.isMessageReady()) {
      int numRead = recvMsg.recv(socketChannel);
      if (numRead == -1) {
        break;
      }
    }
    socketChannel.close();
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(10), recvMsg.getReadOnlyData());
  }

  @Test
  public void readPartialTest1()
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.CACHE, 10);
    long blockId = mTFS.getBlockId(fileId, 0);
    DataServerMessage sendMsg;
    sendMsg = DataServerMessage.createBlockRequestMessage(blockId, 0, 6);
    SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(
        mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mHost,
        mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mPort + 1));
    while (!sendMsg.finishSending()) {
      sendMsg.send(socketChannel);
    }
    DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId, 0, 6);
    while (!recvMsg.isMessageReady()) {
      int numRead = recvMsg.recv(socketChannel);
      if (numRead == -1) {
        break;
      }
    }
    socketChannel.close();
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(6), recvMsg.getReadOnlyData());
  }

  @Test
  public void readPartialTest2()
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.CACHE, 10);
    long blockId = mTFS.getBlockId(fileId, 0);
    DataServerMessage sendMsg;
    sendMsg = DataServerMessage.createBlockRequestMessage(blockId, 2, 6);
    SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(
        mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mHost,
        mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mPort + 1));
    while (!sendMsg.finishSending()) {
      sendMsg.send(socketChannel);
    }
    DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId, 2, 6);
    while (!recvMsg.isMessageReady()) {
      int numRead = recvMsg.recv(socketChannel);
      if (numRead == -1) {
        break;
      }
    }
    socketChannel.close();
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(2, 6), recvMsg.getReadOnlyData());
  }
}
