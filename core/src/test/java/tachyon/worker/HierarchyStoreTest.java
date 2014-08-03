package tachyon.worker;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.StorageId;
import tachyon.StorageLevelAlias;
import tachyon.TestUtils;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.WorkerDirInfo;

/**
 * Unit tests for tachyon.worker.StorageTier.
 */
public class HierarchyStoreTest {
  private final int MEM_CAPACITY_BYTES = 1000;
  private final int DISK_CAPACITY_BYTES = 10000;
  private final int USER_QUOTA_UNIT_BYTES = 100;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTFS = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.worker.hierarchystore.level.max"); // not affect other test cases
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    mLocalTachyonCluster = new LocalTachyonCluster(MEM_CAPACITY_BYTES);
    // String sysTempDir = System.getProperty("java.io.tmpdir", "/tmp");
    System.setProperty("tachyon.worker.hierarchystore.level.max", 2 + "");
    System.setProperty("tachyon.worker.hierarchystore.level1.alias", "HDD");
    // System.setProperty("tachyon.worker.hierarchystore.level1.dirs", sysTempDir + "/disk/1" + ","
    // + sysTempDir + "/disk/2");
    System.setProperty("tachyon.worker.hierarchystore.level1.dirs",
        "/home/shimingfei/disk/1,/home/shimingfei/disk/2");
    System.setProperty("tachyon.worker.hierarchystore.level1.dir.quota", DISK_CAPACITY_BYTES + "");
    mLocalTachyonCluster.start();
    mTFS = mLocalTachyonCluster.getClient();
  }

  @Test
  public void blockEvict() throws IOException, InterruptedException {
    int fileId1 =
        TestUtils.createByteFile(mTFS, "/root/test1", WriteType.TRY_CACHE, MEM_CAPACITY_BYTES / 6);
    int fileId2 =
        TestUtils.createByteFile(mTFS, "/root/test2", WriteType.TRY_CACHE, MEM_CAPACITY_BYTES / 6);
    int fileId3 =
        TestUtils.createByteFile(mTFS, "/root/test3", WriteType.TRY_CACHE, MEM_CAPACITY_BYTES / 6);
    int fileId4 =
        TestUtils.createByteFile(mTFS, "/root/test4", WriteType.TRY_CACHE, MEM_CAPACITY_BYTES / 2);
    int fileId5 =
        TestUtils.createByteFile(mTFS, "/root/test5", WriteType.TRY_CACHE, MEM_CAPACITY_BYTES / 2);

    Thread.sleep(150);

    TachyonFile file1 = mTFS.getFile(fileId1);
    TachyonFile file2 = mTFS.getFile(fileId2);
    TachyonFile file3 = mTFS.getFile(fileId3);
    TachyonFile file4 = mTFS.getFile(fileId4);
    TachyonFile file5 = mTFS.getFile(fileId5);

    Assert.assertEquals(file1.isInMemory(), false);
    Assert.assertEquals(file2.isInMemory(), false);
    Assert.assertEquals(file3.isInMemory(), false);
    Assert.assertEquals(file4.isInMemory(), true);
    Assert.assertEquals(file5.isInMemory(), true);

    WorkerDirInfo dirInfo = mTFS.getDirInfoByBlockId(file1.getBlockId(0));
    long storageId1 = dirInfo.getStorageId();
    dirInfo = mTFS.getDirInfoByBlockId(file2.getBlockId(0));
    long storageId2 = dirInfo.getStorageId();
    dirInfo = mTFS.getDirInfoByBlockId(file3.getBlockId(0));
    long storageId3 = dirInfo.getStorageId();
    dirInfo = mTFS.getDirInfoByBlockId(file4.getBlockId(0));
    long storageId4 = dirInfo.getStorageId();
    dirInfo = mTFS.getDirInfoByBlockId(file5.getBlockId(0));
    long storageId5 = dirInfo.getStorageId();

    Assert.assertEquals(StorageId.getStorageLevelAliasValue(storageId1),
        StorageLevelAlias.HDD.getValue());
    Assert.assertEquals(StorageId.getStorageLevelAliasValue(storageId2),
        StorageLevelAlias.HDD.getValue());
    Assert.assertEquals(StorageId.getStorageLevelAliasValue(storageId3),
        StorageLevelAlias.HDD.getValue());
    Assert.assertEquals(StorageId.getStorageLevelAliasValue(storageId4),
        StorageLevelAlias.MEM.getValue());
    Assert.assertEquals(StorageId.getStorageLevelAliasValue(storageId5),
        StorageLevelAlias.MEM.getValue());
  }

  @Test
  public void promoteBlock() throws IOException, InterruptedException {
    int fileId1 =
        TestUtils.createByteFile(mTFS, "/root/test3", WriteType.TRY_CACHE, MEM_CAPACITY_BYTES / 6);
    int fileId2 =
        TestUtils.createByteFile(mTFS, "/root/test4", WriteType.TRY_CACHE, MEM_CAPACITY_BYTES / 2);
    int fileId3 =
        TestUtils.createByteFile(mTFS, "/root/test5", WriteType.TRY_CACHE, MEM_CAPACITY_BYTES / 2);

    Thread.sleep(150);
    TachyonFile file1 = mTFS.getFile(fileId1);
    TachyonFile file2 = mTFS.getFile(fileId2);
    TachyonFile file3 = mTFS.getFile(fileId3);

    Assert.assertEquals(file1.isInMemory(), false);
    Assert.assertEquals(file2.isInMemory(), true);
    Assert.assertEquals(file3.isInMemory(), true);

    InStream is = file1.getInStream(ReadType.CACHE_PROMOTE);
    byte[] buf = new byte[MEM_CAPACITY_BYTES / 6];
    int len = is.read(buf);

    Thread.sleep(150);
    Assert.assertEquals(len, MEM_CAPACITY_BYTES / 6);
    Assert.assertEquals(file1.isInMemory(), true);
    Assert.assertEquals(file2.isInMemory(), false);
    Assert.assertEquals(file3.isInMemory(), true);
  }
}
