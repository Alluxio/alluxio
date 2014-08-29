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
  public void directByteBufferWriteTest() throws IOException {
    ByteBuffer buf = ByteBuffer.allocateDirect(100);
    buf.put(TestUtils.getIncreasingByteArray(100));

    int fileId = mTfs.createFile("/root/testFile");
    long blockId = mTfs.getBlockId(fileId, 0);
    String localFolder = mTfs.createAndGetUserLocalTempFolder().getPath();
    String filename = CommonUtils.concat(localFolder, blockId);
    BlockHandler handler = BlockHandler.get(filename);
    try {
      handler.append(0, buf);
      mTfs.cacheBlock(blockId);
      TachyonFile file = mTfs.getFile(fileId);
      long fileLen = file.length();
      Assert.assertEquals(100, fileLen);
    } finally {
      handler.close();
    }
    return;
  }

  @Test
  public void heapByteBufferwriteTest() throws IOException {
    int fileId = mTfs.createFile("/root/testFile");
    long blockId = mTfs.getBlockId(fileId, 0);
    String localFolder = mTfs.createAndGetUserLocalTempFolder().getPath();
    String filename = CommonUtils.concat(localFolder, blockId);
    BlockHandler handler = BlockHandler.get(filename);
    byte[] buf = TestUtils.getIncreasingByteArray(100);
    try {
      handler.append(0, ByteBuffer.wrap(buf));
      mTfs.cacheBlock(blockId);
      TachyonFile file = mTfs.getFile(fileId);
      long fileLen = file.length();
      Assert.assertEquals(100, fileLen);
    } finally {
      handler.close();
    }
    return;
  }

  @Test
  public void readExceptionTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, "/root/testFile", WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String filename = file.getLocalFilename(0);
    BlockHandler handler = BlockHandler.get(filename);
    try {
      IllegalArgumentException exception = null;
      ByteBuffer buf = null;
      try {
        buf = handler.read(101, 10);
      } catch (IllegalArgumentException e) {
        exception = e;
      }
      Assert.assertEquals("blockOffset(101) is larger than file length(100)",
          exception.getMessage());
      try {
        buf = handler.read(10, 100);
      } catch (IllegalArgumentException e) {
        exception = e;
      }
      Assert.assertEquals("blockOffset(10) plus length(100) is larger than file length(100)",
          exception.getMessage());
    } finally {
      handler.close();
    }
    return;
  }

  @Test
  public void readTest() throws IOException {
    int fileId = TestUtils.createByteFile(mTfs, "/root/testFile", WriteType.MUST_CACHE, 100);
    TachyonFile file = mTfs.getFile(fileId);
    String filename = file.getLocalFilename(0);
    BlockHandler handler = BlockHandler.get(filename);
    try {
      ByteBuffer buf = handler.read(0, 100);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(100), buf);
      buf = handler.read(0, -1);
      Assert.assertEquals(TestUtils.getIncreasingByteBuffer(100), buf);
    } finally {
      handler.close();
    }
    return;
  }
}
