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

public class BlockHandlerLocalFSTest {

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
    BlockHandler handler = BlockHandler.get(filename, null);
    ByteBuffer buf = handler.readByteBuffer(0, 100);
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(100), buf);
    buf = handler.readByteBuffer(0, -1);
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
    BlockHandler handler = BlockHandler.get(filename, null);
    byte[] buf = TestUtils.getIncreasingByteArray(100);
    handler.appendCurrentBuffer(buf, 0, 0, 100);
    handler.close();
    mTfs.cacheBlock(blockId);
    long fileLen = mTfs.getFileLength(fileId);
    Assert.assertEquals(100, fileLen);
    return;
  }
}
