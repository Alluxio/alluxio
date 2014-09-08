package tachyon.worker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for tachyon.WorkerStorage
 */
public class WorkerStorageTest {
  private static final long WORKER_CAPACITY_BYTES = 100000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
  private InetSocketAddress mMasterAddress = null;
  private NetAddress mWorkerAddress = null;
  private String mWorkerDataFolder = null;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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
    mTfs = mLocalTachyonCluster.getClient();

    mMasterAddress = mLocalTachyonCluster.getMasterAddress();
    mWorkerAddress = mLocalTachyonCluster.getWorkerAddress();
    mWorkerDataFolder = mLocalTachyonCluster.getWorkerDataFolder();
  }

  private void swapoutOrphanBlocksFileTestUtil(int filesize) throws Exception {
    int fid = TestUtils.createByteFile(mTfs, "/xyz", WriteType.MUST_CACHE, filesize);
    long bid = mTfs.getBlockId(fid, 0);
    mLocalTachyonCluster.stopWorker();
    mTfs.delete(fid, true);

    WorkerStorage ws = new WorkerStorage(mMasterAddress, mWorkerDataFolder, WORKER_CAPACITY_BYTES);
    ws.initialize(mWorkerAddress);
    String orpahnblock = ws.getUfsOrphansFolder() + Constants.PATH_SEPARATOR + bid;
    UnderFileSystem ufs = UnderFileSystem.get(orpahnblock);
    Assert.assertFalse("Orphan block file isn't deleted from workerDataFolder", new File(
        mWorkerDataFolder + Constants.PATH_SEPARATOR + bid).exists());
    Assert.assertTrue("UFS hasn't the orphan block file ", ufs.exists(orpahnblock));
    Assert.assertTrue("Orpahblock file size is changed", ufs.getFileSize(orpahnblock) == filesize);
  }

  /**
   * To test the cacheBlock method when multi clients cache the same block.
   * 
   * @throws IOException
   */
  @Test
  public void cacheBlockTest() throws Exception {
    int fileLen = USER_QUOTA_UNIT_BYTES + 4;
    int fid = TestUtils.createByteFile(mTfs, "/cacheBlockTest", WriteType.THROUGH, fileLen);
    long usedBytes = mLocalTachyonCluster.getMasterInfo().getWorkersInfo().get(0).getUsedBytes();
    Assert.assertEquals(0, usedBytes);
    TachyonFS tfs1 = mLocalTachyonCluster.getClient();
    TachyonFS tfs2 = mLocalTachyonCluster.getClient();
    InStream fin1 = tfs1.getFile(fid).getInStream(ReadType.CACHE);
    InStream fin2 = tfs2.getFile(fid).getInStream(ReadType.CACHE);
    for (int i = 0; i < fileLen + 1; i ++) {
      fin1.read();
      fin2.read();
    }
    fin1.close();
    fin2.close();
    usedBytes = mLocalTachyonCluster.getMasterInfo().getWorkersInfo().get(0).getUsedBytes();
    Assert.assertEquals(fileLen, usedBytes);
  }

  /**
   * To test swapout the small file which is bigger than 64K
   * 
   * @throws Exception
   */
  @Test
  public void swapoutOrphanBlocksLargeFileTest() throws Exception {
    swapoutOrphanBlocksFileTestUtil(70000);
  }

  /**
   * To test swapout the small file which is less than 64K
   * 
   * @throws Exception
   */
  @Test
  public void swapoutOrphanBlocksSmallFileTest() throws Exception {
    swapoutOrphanBlocksFileTestUtil(10);
  }

  /**
   * To test initial WorkerStorage with unknown block files
   * 
   * @throws Exception
   */
  @Test
  public void unknownBlockFilesTest() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Wrong file name: xyz");
    mLocalTachyonCluster.stopWorker();
    // try a non-numerical file name
    File unknownFile = new File(mWorkerDataFolder + Constants.PATH_SEPARATOR + "xyz");
    unknownFile.createNewFile();
    WorkerStorage ws = new WorkerStorage(mMasterAddress, mWorkerDataFolder, WORKER_CAPACITY_BYTES);
    ws.initialize(mWorkerAddress);
  }
}
