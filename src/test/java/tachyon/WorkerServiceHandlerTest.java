package tachyon;

import tachyon.client.TachyonClient;
import tachyon.client.OpType;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;

import org.apache.thrift.TException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.ClientFileInfo;

/**
 * Unit tests for WorkerServiceHandler
 */
public class WorkerServiceHandlerTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private WorkerServiceHandler mWorkerServiceHandler = null;
  private TachyonClient mClient = null;
  private final long WORKER_CAPACITY_BYTES = 10000;
  private final int WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS = 5;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS + "");
    System.setProperty("tachyon.worker.to.master.heartbeat.interval.ms",
        WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS + "");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mWorkerServiceHandler = mLocalTachyonCluster.getWorkerServiceHandler();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
    mClient = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.worker.to.master.heartbeat.interval.ms");
  }

  @Test
  public void accessFileTest() throws TException {
    mWorkerServiceHandler.accessFile(1);
    Assert.assertEquals(1, (int) mWorkerServiceHandler.sDataAccessQueue.poll());
    mWorkerServiceHandler.accessFile(1);
    mWorkerServiceHandler.accessFile(2);
    mWorkerServiceHandler.accessFile(3);
    Assert.assertEquals(1, (int) mWorkerServiceHandler.sDataAccessQueue.poll());
    Assert.assertEquals(2, (int) mWorkerServiceHandler.sDataAccessQueue.poll());
    Assert.assertEquals(3, (int) mWorkerServiceHandler.sDataAccessQueue.poll());
    mWorkerServiceHandler.accessFile(1);
    mWorkerServiceHandler.accessFile(1);
    mWorkerServiceHandler.accessFile(2);
    Assert.assertEquals(1, (int) mWorkerServiceHandler.sDataAccessQueue.poll());
    Assert.assertEquals(1, (int) mWorkerServiceHandler.sDataAccessQueue.poll());
    Assert.assertEquals(2, (int) mWorkerServiceHandler.sDataAccessQueue.poll());
  }

  @Test
  public void overCapacityRequestSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES / 10L));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES * 10L));
  }

  @Test
  public void totalOverCapacityRequestSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 2));
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 2));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 2));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 2));
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
  public void overReturnSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 10));
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 10));
    mWorkerServiceHandler.returnSpace(1, WORKER_CAPACITY_BYTES);
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES));
  }

  @Test
  public void evictionTest() 
      throws InvalidPathException, FileAlreadyExistException, IOException,
      FileDoesNotExistException, TException {
    int fileId1 = TestUtils.createSimpleFile(
        mClient, "/file1", OpType.WRITE_CACHE, (int) WORKER_CAPACITY_BYTES / 3);
    Assert.assertTrue(fileId1 >= 0);
    ClientFileInfo fileInfo1 = mMasterInfo.getFileInfo("/file1");
    Assert.assertTrue(fileInfo1.isInMemory());
    int fileId2 = TestUtils.createSimpleFile(
        mClient, "/file2", OpType.WRITE_CACHE, (int) WORKER_CAPACITY_BYTES / 3);
    Assert.assertTrue(fileId2 >= 0);
    fileInfo1 = mMasterInfo.getFileInfo("/file1");
    ClientFileInfo fileInfo2 = mMasterInfo.getFileInfo("/file2");
    Assert.assertTrue(fileInfo1.isInMemory());
    Assert.assertTrue(fileInfo2.isInMemory());
    int fileId3 = TestUtils.createSimpleFile(
        mClient, "/file3", OpType.WRITE_CACHE, (int) WORKER_CAPACITY_BYTES / 2);
    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    fileInfo1 = mMasterInfo.getFileInfo("/file1");
    fileInfo2 = mMasterInfo.getFileInfo("/file2");
    ClientFileInfo fileInfo3 = mMasterInfo.getFileInfo("/file3");
    Assert.assertTrue(fileId3 >= 0);
    Assert.assertFalse(fileInfo1.isInMemory());
    Assert.assertTrue(fileInfo2.isInMemory());
    Assert.assertTrue(fileInfo3.isInMemory());
  }
}