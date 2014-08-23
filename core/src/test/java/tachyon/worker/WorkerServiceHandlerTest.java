package tachyon.worker;

import java.io.IOException;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;

/**
 * Unit tests for tachyon.WorkerServiceHandler
 */
public class WorkerServiceHandlerTest {
  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;
  private static final int WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS =
      WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private WorkerServiceHandler mWorkerServiceHandler = null;
  private TachyonFS mTfs = null;

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
    mWorkerServiceHandler = mLocalTachyonCluster.getWorker().getWorkerServiceHandler();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
    mTfs = mLocalTachyonCluster.getClient();
  }

  @Test
  public void evictionTest() throws InvalidPathException, FileAlreadyExistException, IOException,
      FileDoesNotExistException, TException {
    int fileId1 =
        TestUtils.createByteFile(mTfs, "/file1", WriteType.MUST_CACHE,
            (int) WORKER_CAPACITY_BYTES / 3);
    Assert.assertTrue(fileId1 >= 0);
    ClientFileInfo fileInfo1 = mMasterInfo.getClientFileInfo("/file1");
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);
    int fileId2 =
        TestUtils.createByteFile(mTfs, "/file2", WriteType.MUST_CACHE,
            (int) WORKER_CAPACITY_BYTES / 3);
    Assert.assertTrue(fileId2 >= 0);
    fileInfo1 = mMasterInfo.getClientFileInfo("/file1");
    ClientFileInfo fileInfo2 = mMasterInfo.getClientFileInfo("/file2");
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo2.inMemoryPercentage);
    int fileId3 =
        TestUtils.createByteFile(mTfs, "/file3", WriteType.MUST_CACHE,
            (int) WORKER_CAPACITY_BYTES / 2);
    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    fileInfo1 = mMasterInfo.getClientFileInfo("/file1");
    fileInfo2 = mMasterInfo.getClientFileInfo("/file2");
    ClientFileInfo fileInfo3 = mMasterInfo.getClientFileInfo("/file3");
    Assert.assertTrue(fileId3 >= 0);
    Assert.assertEquals(0, fileInfo1.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo2.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo3.inMemoryPercentage);
  }

  @Test
  public void overCapacityRequestSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES / 10L));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES * 10L));
  }

  @Test
  public void overReturnSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 10));
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 10));
    mWorkerServiceHandler.returnSpace(1, WORKER_CAPACITY_BYTES);
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES));
  }

  @Test
  public void returnSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES));
    mWorkerServiceHandler.returnSpace(1, WORKER_CAPACITY_BYTES);
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES));
    mWorkerServiceHandler.returnSpace(2, WORKER_CAPACITY_BYTES);
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 10));
  }

  @Test
  public void totalOverCapacityRequestSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 2));
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 2));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 2));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 2));
  }
}