package tachyon.worker.hierarchy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.BlockHandler;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;

public class StorageDirTest {
  private StorageDir mSrcDir;
  private StorageDir mDstDir;
  private final long USER_ID = 1;
  private final long CAPACITY = 1000;

  @Before
  public final void before() throws IOException, InvalidPathException {
    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();
    String workerDirFolder = tachyonHome + "/ramdisk";
    mSrcDir = new StorageDir(1, workerDirFolder + "/src", CAPACITY, "/data", "/user", null);
    mDstDir = new StorageDir(2, workerDirFolder + "/dst", CAPACITY, "/data", "/user", null);

    UnderFileSystem srcUfs = mSrcDir.getUfs();
    srcUfs.mkdirs(mSrcDir.getDirDataPath(), true);
    srcUfs.mkdirs(mSrcDir.getUserTempPath(USER_ID), true);
    CommonUtils.changeLocalFileToFullPermission(mSrcDir.getDirDataPath());
    CommonUtils.changeLocalFileToFullPermission(mSrcDir.getUserTempPath(USER_ID));

    UnderFileSystem dstUfs = mDstDir.getUfs();
    dstUfs.mkdirs(mDstDir.getDirDataPath(), true);
    dstUfs.mkdirs(mDstDir.getUserTempPath(USER_ID), true);
    CommonUtils.changeLocalFileToFullPermission(mDstDir.getDirDataPath());
    CommonUtils.changeLocalFileToFullPermission(mDstDir.getUserTempPath(USER_ID));
  }

  @Test
  public void copyBlockTest() throws IOException {
    long blockId = 100;
    long blockSize = 500;

    createBlockFile(mSrcDir, blockId, blockSize);
    Assert.assertTrue(mSrcDir.containsBlock(blockId));
    Assert.assertEquals(CAPACITY - blockSize, mSrcDir.getAvailable());
    Assert.assertEquals(blockSize, mSrcDir.getBlockSize(blockId));
    boolean requestDst = mDstDir.requestSpace(USER_ID, blockSize);
    Assert.assertTrue(requestDst);
    mSrcDir.copyBlock(blockId, mDstDir);
    Assert.assertTrue(mDstDir.containsBlock(blockId));
    Assert.assertEquals(CAPACITY - blockSize, mDstDir.getAvailable());
    Assert.assertEquals(blockSize, mDstDir.getBlockSize(blockId));
    mSrcDir.deleteBlock(blockId);
    Assert.assertFalse(mSrcDir.containsBlock(blockId));
    Assert.assertEquals(CAPACITY, mSrcDir.getAvailable());
  }

  private void createBlockFile(StorageDir dir, long blockId, long blockSize) throws IOException {
    byte[] buf = TestUtils.getIncreasingByteArray((int) blockSize);
    BlockHandler bhSrc =
        BlockHandler.get(CommonUtils.concat(mSrcDir.getUserTempFilePath(USER_ID, blockId)));
    bhSrc.append(0, ByteBuffer.wrap(buf));
    bhSrc.close();
    dir.requestSpace(USER_ID, blockSize);
    dir.cacheBlock(USER_ID, blockId);
  }

  @Test
  public void getBlockDataTest() throws IOException {
    long blockId = 100;
    long blockSize = 500;
    createBlockFile(mSrcDir, blockId, blockSize);
    byte[] buf = TestUtils.getIncreasingByteArray((int) blockSize);
    ByteBuffer dataBuf = mSrcDir.getBlockData(blockId, blockSize / 2, blockSize / 2);
    Assert.assertEquals(ByteBuffer.wrap(buf, (int) blockSize / 2, (int) blockSize / 2), dataBuf);
    dataBuf = mSrcDir.getBlockData(blockId, 0, -1);
    Assert.assertEquals(ByteBuffer.wrap(buf), dataBuf);
  }

  @Test
  public void lockBlockTest() throws IOException {
    long blockId = 100;
    createBlockFile(mSrcDir, blockId, 500);
    mSrcDir.lockBlock(blockId, USER_ID);
    Assert.assertTrue(mSrcDir.getUsersPerLockedBlock().containsKey(blockId));
    mSrcDir.unlockBlock(blockId, USER_ID);
    Assert.assertFalse(mSrcDir.getUsersPerLockedBlock().containsKey(blockId));
  }

  @Test
  public void moveBlockTest() throws IOException {
    long blockId = 100;
    int blockSize = 500;
    createBlockFile(mSrcDir, blockId, blockSize);
    mDstDir.requestSpace(USER_ID, blockSize);
    mSrcDir.moveBlock(blockId, mDstDir);
    Assert.assertFalse(mSrcDir.containsBlock(blockId));
    Assert.assertTrue(mDstDir.containsBlock(blockId));
    Assert.assertEquals(blockSize, mDstDir.getBlockSize(blockId));
  }

  @Test
  public void requestSpaceTest() {
    boolean requestSrc = mSrcDir.requestSpace(USER_ID, CAPACITY / 2);
    Assert.assertTrue(requestSrc);
    requestSrc = mSrcDir.requestSpace(USER_ID, CAPACITY / 2 + 1);
    Assert.assertFalse(requestSrc);
    Assert.assertEquals(CAPACITY / 2, mSrcDir.getUsed());
    Assert.assertEquals(CAPACITY / 2, mSrcDir.getAvailable());
    mSrcDir.returnSpace(USER_ID, CAPACITY / 2);
    Assert.assertEquals(CAPACITY, mSrcDir.getAvailable());
  }
}
