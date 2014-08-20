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

import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.worker.nio.DataServerMessage;

/**
 * Unit tests for tachyon.DataServer.
 */
@RunWith(Parameterized.class)
public class DataServerTest {
  private static final int WORKER_CAPACITY_BYTES = 1000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  private final NetworkType type;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTFS = null;

  public DataServerTest(NetworkType type) {
    this.type = type;
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.worker.network.type");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    System.setProperty("tachyon.worker.network.type", type.toString());
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mTFS = mLocalTachyonCluster.getClient();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // creates a new instance of DataServerTest for each network type
    List<Object[]> list = new ArrayList<Object[]>();
    for (final NetworkType type : NetworkType.values()) {
      list.add(new Object[] { type });
    }
    return list;
  }

  @Test
  public void readPartialTest1() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    final int offset = 0;
    final int length = 6;
    DataServerMessage recvMsg = request(block, offset, length);
    assertValid(recvMsg, length, block.getBlockId(), offset, length);
  }

  @Test
  public void readPartialTest2() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
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
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block);
    assertValid(recvMsg, length, block.getBlockId(), 0, length);
  }

  @Test
  public void multiReadTest() throws IOException {
    final int length = 20;
    int fileId = TestUtils.createByteFile(mTFS, "/multiReadTest", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    for (int i = 0; i < 10; i ++) {
      DataServerMessage recvMsg = request(block);
      assertValid(recvMsg, length, block.getBlockId(), 0, length);
    }
  }

  @Test
  public void readTooLarge() throws IOException {
    final int length = 20;
    int fileId = TestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block, 0, length * 2);
    assertError(recvMsg, block.blockId);
  }

  @Test
  public void lengthTooSmall() throws IOException {
    final int length = 20;
    int fileId = TestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block, 0, length * -2);
    assertError(recvMsg, block.blockId);
  }

  @Test
  public void tooLargeOffset() throws IOException {
    final int length = 10;
    int fileId = TestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block, length * 2, 1);
    assertError(recvMsg, block.blockId);
  }

  @Test
  public void negativeOffset() throws IOException {
    final int length = 10;
    int fileId = TestUtils.createByteFile(mTFS, "/readTooLarge", WriteType.MUST_CACHE, length);
    ClientBlockInfo block = mTFS.getFileBlocks(fileId).get(0);
    DataServerMessage recvMsg = request(block, length * -2, 1);
    assertError(recvMsg, block.blockId);
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
  private void assertValid(final DataServerMessage msg, final int expectedSize,
      final long blockId, final long offset, final long length) {
    assertValid(msg, TestUtils.getIncreasingByteBuffer(expectedSize), blockId, offset, length);
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
   * Requests a block from the server.  This call will read the full block.
   */
  private DataServerMessage request(final ClientBlockInfo block) throws IOException {
    return request(block, 0, -1);
  }

  /**
   * Create a new socket to the data port and send a block request.  The returned value is
   * the response from the server.
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
          DataServerMessage.createBlockResponseMessage(false, block.blockId, offset, length);
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
}
