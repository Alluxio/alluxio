package tachyon.worker.hierarchy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Pair;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.BlockHandler;
import tachyon.master.BlockInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;
import tachyon.worker.eviction.EvictLRU;
import tachyon.worker.eviction.EvictPartialLRU;
import tachyon.worker.eviction.EvictStrategy;

public class EvictStrategyTest {
  private final StorageDir[] mStorageDirs = new StorageDir[3];
  private final long mUserId = 1;
  private final long mCapacity = 1000;

  @Before
  public final void before() throws IOException, InvalidPathException, InterruptedException {
    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();
    String workerDirFolder = tachyonHome + "/ramdisk";
    String[] dirPaths = "/dir1,/dir2,/dir3".split(",");
    for (int i = 0; i < 3; i++) {
      mStorageDirs[i] =
          new StorageDir(i + 1, workerDirFolder + dirPaths[i], mCapacity, "/data", "/user", null);
      initializeStorageDir(mStorageDirs[i], mUserId);
    }
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
  public void EvictLRUTest() throws IOException, InterruptedException {
    createBlockFile(mStorageDirs[0], BlockInfo.computeBlockId(1, 0), 300);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[1], BlockInfo.computeBlockId(2, 0), 300);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[2], BlockInfo.computeBlockId(3, 0), 350);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[0], BlockInfo.computeBlockId(4, 0), 550);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[1], BlockInfo.computeBlockId(5, 0), 600);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[2], BlockInfo.computeBlockId(6, 0), 500);

    Set<Integer> pinList = new HashSet<Integer>();
    EvictStrategy eviction = new EvictLRU(true);

    pinList.add(1);
    Pair<StorageDir, List<tachyon.worker.hierarchy.BlockInfo>> lruResult =
        eviction.getDirCandidate(mStorageDirs, pinList, 300);
    Assert.assertEquals(mStorageDirs[1], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(2, 0));

    pinList.add(2);
    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 300);
    Assert.assertEquals(mStorageDirs[2], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(3, 0));

    eviction = new EvictLRU(false);
    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 300);
    Assert.assertEquals(mStorageDirs[0], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(1, 0));

    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 1001);
    Assert.assertEquals(null, lruResult);
  }

  @Test
  public void EvictPartialLRUTest() throws IOException, InterruptedException {
    createBlockFile(mStorageDirs[0], BlockInfo.computeBlockId(1, 0), 450);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[1], BlockInfo.computeBlockId(2, 0), 600);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[2], BlockInfo.computeBlockId(3, 0), 500);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[0], BlockInfo.computeBlockId(4, 0), 400);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[1], BlockInfo.computeBlockId(5, 0), 300);
    Thread.sleep(10);
    createBlockFile(mStorageDirs[2], BlockInfo.computeBlockId(6, 0), 450);

    Set<Integer> pinList = new HashSet<Integer>();
    EvictStrategy eviction = new EvictPartialLRU(true);

    pinList.add(1);
    Pair<StorageDir, List<tachyon.worker.hierarchy.BlockInfo>> lruResult =
        eviction.getDirCandidate(mStorageDirs, pinList, 600);
    Assert.assertEquals(mStorageDirs[1], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(2, 0));

    pinList.add(2);
    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 600);
    Assert.assertEquals(mStorageDirs[2], lruResult.getFirst());
    Assert.assertEquals(2, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(3, 0));
    Assert.assertEquals(lruResult.getSecond().get(1).getBlockId(), BlockInfo.computeBlockId(6, 0));

    eviction = new EvictLRU(false);
    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 400);
    Assert.assertEquals(mStorageDirs[0], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(1, 0));

    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 1001);
    Assert.assertEquals(null, lruResult);
  }

  private void initializeStorageDir(StorageDir dir, long userId) throws IOException {
    dir.initailize();
    UnderFileSystem ufs = dir.getUfs();
    ufs.mkdirs(dir.getUserTempPath(userId), true);
    CommonUtils.changeLocalFileToFullPermission(dir.getUserTempPath(userId));
  }
}
