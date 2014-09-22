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
  private final long mUserId = 1;
  private final long mCapacity = 1000;

  @Before
  public final void before() throws IOException, InvalidPathException {
    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();
    String workerDirFolder = tachyonHome + "/ramdisk";
    mSrcDir = new StorageDir(1, workerDirFolder + "/src", mCapacity, "/data", "/user", null);
    mDstDir = new StorageDir(2, workerDirFolder + "/dst", mCapacity, "/data", "/user", null);

    initializeStorageDir(mSrcDir, mUserId);
    initializeStorageDir(mDstDir, mUserId);
  }

  @Test
  public void copyBlockTest() throws IOException {
    long blockId = 100;
    int blockSize = 500;

    createBlockFile(mSrcDir, blockId, blockSize);
    Assert.assertTrue(mSrcDir.containsBlock(blockId));
    Assert.assertEquals(mCapacity - blockSize, mSrcDir.getAvailableBytes());
    Assert.assertEquals(blockSize, mSrcDir.getBlockSize(blockId));
    boolean requestDst = mDstDir.requestSpace(mUserId, blockSize);
    Assert.assertTrue(requestDst);
    mSrcDir.copyBlock(blockId, mDstDir);
    Assert.assertTrue(mDstDir.containsBlock(blockId));
    Assert.assertEquals(mCapacity - blockSize, mDstDir.getAvailableBytes());
    Assert.assertEquals(blockSize, mDstDir.getBlockSize(blockId));
    mSrcDir.deleteBlock(blockId);
    Assert.assertFalse(mSrcDir.containsBlock(blockId));
    Assert.assertEquals(mCapacity, mSrcDir.getAvailableBytes());
  }

  private void createBlockFile(StorageDir dir, long blockId, int blockSize) throws IOException {
    byte[] buf = TestUtils.getIncreasingByteArray(blockSize);

    BlockHandler bhSrc =
        BlockHandler.get(CommonUtils.concat(dir.getUserTempFilePath(mUserId, blockId)));
    try {
      bhSrc.append(0, ByteBuffer.wrap(buf));
    } finally {
      bhSrc.close();
    }
    dir.requestSpace(mUserId, blockSize);
    dir.cacheBlock(mUserId, blockId);
  }

  @Test
  public void getBlockDataTest() throws IOException {
    long blockId = 100;
    int blockSize = 500;

    createBlockFile(mSrcDir, blockId, blockSize);
    byte[] buf = TestUtils.getIncreasingByteArray(blockSize);
    ByteBuffer dataBuf = mSrcDir.getBlockData(blockId, blockSize / 2, blockSize / 2);
    Assert.assertEquals(ByteBuffer.wrap(buf, blockSize / 2, blockSize / 2), dataBuf);
    dataBuf = mSrcDir.getBlockData(blockId, 0, -1);
    Assert.assertEquals(ByteBuffer.wrap(buf), dataBuf);
  }

  private void initializeStorageDir(StorageDir dir, long userId) throws IOException {
    dir.initailize();
    UnderFileSystem ufs = dir.getUfs();
    ufs.mkdirs(dir.getUserTempPath(userId), true);
    CommonUtils.changeLocalFileToFullPermission(dir.getUserTempPath(userId));
  }

  @Test
  public void lockBlockTest() throws IOException {
    long blockId = 100;

    createBlockFile(mSrcDir, blockId, 500);
    mSrcDir.lockBlock(blockId, mUserId);
    Assert.assertTrue(mSrcDir.isBlockLocked(blockId));
    mSrcDir.unlockBlock(blockId, mUserId);
    Assert.assertFalse(mSrcDir.isBlockLocked(blockId));
  }

  @Test
  public void moveBlockTest() throws IOException {
    long blockId = 100;
    int blockSize = 500;

    createBlockFile(mSrcDir, blockId, blockSize);
    mDstDir.requestSpace(mUserId, blockSize);
    mSrcDir.moveBlock(blockId, mDstDir);
    Assert.assertFalse(mSrcDir.containsBlock(blockId));
    Assert.assertTrue(mDstDir.containsBlock(blockId));
    Assert.assertEquals(blockSize, mDstDir.getBlockSize(blockId));
  }

  @Test
  public void requestSpaceTest() {
    boolean requestSrc = mSrcDir.requestSpace(mUserId, mCapacity / 2);
    Assert.assertTrue(requestSrc);
    requestSrc = mSrcDir.requestSpace(mUserId, mCapacity / 2 + 1);
    Assert.assertFalse(requestSrc);
    Assert.assertEquals(mCapacity / 2, mSrcDir.getUsedBytes());
    Assert.assertEquals(mCapacity / 2, mSrcDir.getAvailableBytes());
    mSrcDir.returnSpace(mUserId, mCapacity / 2);
    Assert.assertEquals(mCapacity, mSrcDir.getAvailableBytes());
  }
}
