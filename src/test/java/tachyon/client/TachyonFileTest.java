package tachyon.client;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.CommonUtils;
import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests for tachyon.client.TachyonFile.
 */
public class TachyonFileTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mClient = null;
  private final int WORKER_CAPACITY_BYTES = 1000;
  private final int USER_QUOTA_UNIT_BYTES = 100;
  private final int WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS = 5;
  private final String PIN_DATA = "/pin";
  private final int MAX_FILES = WORKER_CAPACITY_BYTES / USER_QUOTA_UNIT_BYTES;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    System.setProperty("tachyon.worker.to.master.heartbeat.interval.ms",
        WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS + "");
    System.setProperty("tachyon.master.pinlist", PIN_DATA);
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.worker.to.master.heartbeat.interval.ms");
    System.clearProperty("tachyon.master.pinlist");
  }

  /**
   * Basic isInMemory Test.
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws IOException
   */
  @Test
  public void isInMemoryTest() throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = TestUtils.createByteFile(
        mClient, "/file1", OpType.WRITE_CACHE, USER_QUOTA_UNIT_BYTES);
    TachyonFile file = mClient.getFile(fileId);
    Assert.assertTrue(file.isInMemory());

    fileId = TestUtils.createByteFile(
        mClient, "/file2", OpType.WRITE_CACHE_THROUGH, USER_QUOTA_UNIT_BYTES);
    file = mClient.getFile(fileId);
    Assert.assertTrue(file.isInMemory());

    fileId = TestUtils.createByteFile(
        mClient, "/file3", OpType.WRITE_THROUGH, USER_QUOTA_UNIT_BYTES);
    file = mClient.getFile(fileId);
    Assert.assertFalse(file.isInMemory());
    Assert.assertTrue(file.recacheData());
    Assert.assertTrue(file.isInMemory());

    fileId = TestUtils.createByteFile(
        mClient, "/file4", OpType.WRITE_THROUGH, WORKER_CAPACITY_BYTES + 1);
    file = mClient.getFile(fileId);
    Assert.assertFalse(file.isInMemory());
    Assert.assertFalse(file.recacheData());
    Assert.assertFalse(file.isInMemory());

    fileId = TestUtils.createByteFile(
        mClient, "/file5", OpType.WRITE_THROUGH, WORKER_CAPACITY_BYTES);
    file = mClient.getFile(fileId);
    Assert.assertFalse(file.isInMemory());
    Assert.assertTrue(file.recacheData());
    Assert.assertTrue(file.isInMemory());
  }

  /**
   * Test LRU Cache Eviction.
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws IOException
   */
  @Test
  public void isInMemoryTest2() 
      throws InvalidPathException, FileAlreadyExistException, IOException {
    for (int k = 0; k < MAX_FILES; k ++) {
      int fileId = TestUtils.createByteFile(
          mClient, "/file" + k, OpType.WRITE_CACHE, USER_QUOTA_UNIT_BYTES);
      TachyonFile file = mClient.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
    }

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    for (int k = 0; k < MAX_FILES; k ++) {
      TachyonFile file = mClient.getFile("/file" + k);
      Assert.assertTrue(file.isInMemory());
    }

    for (int k = MAX_FILES; k < MAX_FILES + 1; k ++) {
      int fileId = TestUtils.createByteFile(
          mClient, "/file" + k, OpType.WRITE_CACHE, USER_QUOTA_UNIT_BYTES);
      TachyonFile file = mClient.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
    }

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    TachyonFile file = mClient.getFile("/file" + 0);
    Assert.assertFalse(file.isInMemory());

    for (int k = 1; k < MAX_FILES + 1; k ++) {
      file = mClient.getFile("/file" + k);
      Assert.assertTrue(file.isInMemory());
    }
  }

  /**
   * Test LRU Cache Eviction + PIN.
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws IOException
   */
  @Test
  public void isInMemoryTest3() 
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = TestUtils.createByteFile(
        mClient, "/pin/file", OpType.WRITE_CACHE, USER_QUOTA_UNIT_BYTES);
    TachyonFile file = mClient.getFile(fileId);
    Assert.assertTrue(file.isInMemory());

    for (int k = 0; k < MAX_FILES; k ++) {
      fileId = TestUtils.createByteFile(
          mClient, "/file" + k, OpType.WRITE_CACHE, USER_QUOTA_UNIT_BYTES);
      file = mClient.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
    }

    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);

    file = mClient.getFile("/pin/file");
    Assert.assertTrue(file.isInMemory());
    file = mClient.getFile("/file0");
    Assert.assertFalse(file.isInMemory());
    for (int k = 1; k < MAX_FILES; k ++) {
      file = mClient.getFile("/file" + k);
      Assert.assertTrue(file.isInMemory());
    }
  }
}
