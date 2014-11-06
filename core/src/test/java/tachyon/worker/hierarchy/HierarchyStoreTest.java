package tachyon.worker.hierarchy;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.TestUtils;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.WorkerFileInfo;
import tachyon.util.CommonUtils;

/**
 * Unit tests for tachyon.worker.StorageTier.
 */
public class HierarchyStoreTest {
  private final int mMemCapacityBytes = 1000;
  private final int mDiskCapacityBytes = 10000;
  private final int mUserQuotaUnitBytes = 100;
  private final int WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS
      = WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTFS = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.worker.hierarchystore.level.max");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", mUserQuotaUnitBytes + "");
    mLocalTachyonCluster = new LocalTachyonCluster(mMemCapacityBytes);
    System.setProperty("tachyon.worker.hierarchystore.level.max", 2 + "");
    System.setProperty("tachyon.worker.hierarchystore.level1.alias", "HDD");
    System.setProperty("tachyon.worker.hierarchystore.level1.dirs.path", "/disk1" + "," + "/disk2");
    System.setProperty("tachyon.worker.hierarchystore.level1.dirs.quota", mDiskCapacityBytes + "");
    mLocalTachyonCluster.start();
    mTFS = mLocalTachyonCluster.getClient();
  }

  @Test
  public void blockEvict() throws IOException, InterruptedException {
    int fileId1 =
        TestUtils.createByteFile(mTFS, "/root/test1", WriteType.TRY_CACHE, mMemCapacityBytes / 6);
    int fileId2 =
        TestUtils.createByteFile(mTFS, "/root/test2", WriteType.TRY_CACHE, mMemCapacityBytes / 6);
    int fileId3 =
        TestUtils.createByteFile(mTFS, "/root/test3", WriteType.TRY_CACHE, mMemCapacityBytes / 6);

    TachyonFile file1 = mTFS.getFile(fileId1);
    TachyonFile file2 = mTFS.getFile(fileId2);
    TachyonFile file3 = mTFS.getFile(fileId3);

    Assert.assertEquals(file1.isInMemory(), true);
    Assert.assertEquals(file2.isInMemory(), true);
    Assert.assertEquals(file3.isInMemory(), true);

    int fileId4 =
        TestUtils.createByteFile(mTFS, "/root/test4", WriteType.TRY_CACHE, mMemCapacityBytes / 2);
    int fileId5 =
        TestUtils.createByteFile(mTFS, "/root/test5", WriteType.TRY_CACHE, mMemCapacityBytes / 2);

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    TachyonFile file4 = mTFS.getFile(fileId4);
    TachyonFile file5 = mTFS.getFile(fileId5);

    Assert.assertEquals(file1.isInMemory(), false);
    Assert.assertEquals(file2.isInMemory(), false);
    Assert.assertEquals(file3.isInMemory(), false);
    Assert.assertEquals(file4.isInMemory(), true);
    Assert.assertEquals(file5.isInMemory(), true);

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    WorkerFileInfo fileInfo = mTFS.getBlockFileInfo(file1.getBlockId(0));
    long storageDirId1 = fileInfo.getStorageDirId();
    fileInfo = mTFS.getBlockFileInfo(file2.getBlockId(0));
    long storageDirId2 = fileInfo.getStorageDirId();
    fileInfo = mTFS.getBlockFileInfo(file3.getBlockId(0));
    long storageDirId3 = fileInfo.getStorageDirId();
    fileInfo = mTFS.getBlockFileInfo(file4.getBlockId(0));
    long storageDirId4 = fileInfo.getStorageDirId();
    fileInfo = mTFS.getBlockFileInfo(file5.getBlockId(0));
    long storageDirId5 = fileInfo.getStorageDirId();

    Assert.assertEquals(StorageLevelAlias.HDD.getValue(),
        StorageDirId.getStorageLevelAliasValue(storageDirId1));
    Assert.assertEquals(StorageLevelAlias.HDD.getValue(),
        StorageDirId.getStorageLevelAliasValue(storageDirId2));
    Assert.assertEquals(StorageLevelAlias.HDD.getValue(),
        StorageDirId.getStorageLevelAliasValue(storageDirId3));
    Assert.assertEquals(StorageLevelAlias.MEM.getValue(),
        StorageDirId.getStorageLevelAliasValue(storageDirId4));
    Assert.assertEquals(StorageLevelAlias.MEM.getValue(),
        StorageDirId.getStorageLevelAliasValue(storageDirId5));
  }

  @Test
  public void promoteBlock() throws IOException, InterruptedException {
    int fileId1 =
        TestUtils.createByteFile(mTFS, "/root/test1", WriteType.TRY_CACHE, mMemCapacityBytes / 6);
    int fileId2 =
        TestUtils.createByteFile(mTFS, "/root/test2", WriteType.TRY_CACHE, mMemCapacityBytes / 2);
    int fileId3 =
        TestUtils.createByteFile(mTFS, "/root/test3", WriteType.TRY_CACHE, mMemCapacityBytes / 2);

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    TachyonFile file1 = mTFS.getFile(fileId1);
    TachyonFile file2 = mTFS.getFile(fileId2);
    TachyonFile file3 = mTFS.getFile(fileId3);

    Assert.assertEquals(false, file1.isInMemory());
    Assert.assertEquals(true, file2.isInMemory());
    Assert.assertEquals(true, file3.isInMemory());

    InStream is = file1.getInStream(ReadType.CACHE_PROMOTE);
    byte[] buf = new byte[mMemCapacityBytes / 6];
    int len = is.read(buf);

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    Assert.assertEquals(mMemCapacityBytes / 6, len);
    Assert.assertEquals(true, file1.isInMemory());
    Assert.assertEquals(false, file2.isInMemory());
    Assert.assertEquals(true, file3.isInMemory());
  }
}
