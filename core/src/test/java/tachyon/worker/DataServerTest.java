package tachyon.worker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for tachyon.DataServer.
 */
public class DataServerTest {
  private static final int WORKER_CAPACITY_BYTES = 1000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTFS = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mTFS = mLocalTachyonCluster.getClient();
  }

  @Test
  public void readPartialTest1() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    long blockId = mTFS.getBlockId(fileId, 0);
    DataServerMessage sendMsg;
    sendMsg = DataServerMessage.createBlockRequestMessage(blockId, 0, 6);
    NetAddress firstBlock = mTFS.getFileBlocks(fileId).get(0).getLocations().get(0);
    SocketChannel socketChannel =
        SocketChannel.open(new InetSocketAddress(firstBlock.mHost, firstBlock.mSecondaryPort));
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
  public void readPartialTest2() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    long blockId = mTFS.getBlockId(fileId, 0);
    DataServerMessage sendMsg;
    sendMsg = DataServerMessage.createBlockRequestMessage(blockId, 2, 6);
    SocketChannel socketChannel =
        SocketChannel
            .open(new InetSocketAddress(
                mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mHost, mTFS
                    .getFileBlocks(fileId).get(0).getLocations().get(0).mSecondaryPort));
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

  @Test
  public void readTest() throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    long blockId = mTFS.getBlockId(fileId, 0);
    DataServerMessage sendMsg = DataServerMessage.createBlockRequestMessage(blockId);
    SocketChannel socketChannel =
        SocketChannel
            .open(new InetSocketAddress(
                mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mHost, mTFS
                    .getFileBlocks(fileId).get(0).getLocations().get(0).mSecondaryPort));
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
}
