package tachyon;

import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;
import tachyon.client.OutStream;
import tachyon.client.OpType;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import org.apache.thrift.TException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.FailedToCheckpointException;
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

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
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
  }
  
  /**
   * Create a simple file with length <code>len</code>.
   * @param len
   * @return file id of the new created file.
   * @throws FileAlreadyExistException 
   * @throws InvalidPathException 
   * @throws IOException 
   */
  private int createSimpleFile(String fileName, OpType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mClient.createFile(fileName);
    TachyonFile file = mClient.getFile(fileId);
    OutStream os = file.createOutStream(op);

    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();

    return fileId;
  }

    
  @Test
  public void accessFileTest() throws TException {
    mWorkerServiceHandler.accessFile(1);
    Assert.assertTrue(mWorkerServiceHandler.sDataAccessQueue.poll() == 1);
    mWorkerServiceHandler.accessFile(1);
    mWorkerServiceHandler.accessFile(2);
    mWorkerServiceHandler.accessFile(3);
    Assert.assertTrue(mWorkerServiceHandler.sDataAccessQueue.poll() == 1);
    Assert.assertTrue(mWorkerServiceHandler.sDataAccessQueue.poll() == 2);
    Assert.assertTrue(mWorkerServiceHandler.sDataAccessQueue.poll() == 3);
    mWorkerServiceHandler.accessFile(1);
    mWorkerServiceHandler.accessFile(1);
    mWorkerServiceHandler.accessFile(2);
    Assert.assertTrue(mWorkerServiceHandler.sDataAccessQueue.poll() == 1);
    Assert.assertTrue(mWorkerServiceHandler.sDataAccessQueue.poll() == 1);
    Assert.assertTrue(mWorkerServiceHandler.sDataAccessQueue.poll() == 2);
  }
  
  @Test @Ignore // not sure about this
  public void addCheckpointTest() 
      throws FileDoesNotExistException, SuspectedFileSizeException, InvalidPathException,
      FailedToCheckpointException, TException, FileAlreadyExistException {
    int fileId = mMasterInfo.createFile("/testFile", false);
    mWorkerServiceHandler.addCheckpoint(1L, fileId);
    ClientFileInfo fileInfo = mMasterInfo.getFileInfo("/testFile");
    Assert.assertFalse(fileInfo.getCheckpointPath().equals(""));
  }    
  
  @Test
  public void overCapacityRequestSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES/10L));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES*10L));
  }
  
  @Test
  public void totalOverCapacityRequestSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES/2L));
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(2L, WORKER_CAPACITY_BYTES/2L));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES/2L));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(2L, WORKER_CAPACITY_BYTES/2L));
  }
  
  @Test // Seems there is a bug
  public void returnSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES));
    mWorkerServiceHandler.returnSpace(1L, WORKER_CAPACITY_BYTES);
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES));
    mWorkerServiceHandler.returnSpace(2L, WORKER_CAPACITY_BYTES);
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(2L, WORKER_CAPACITY_BYTES/10L));
  }
  
  @Test @Ignore // Seems there is a bug
  public void overReturnSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES/10L));
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(2L, WORKER_CAPACITY_BYTES/10L));
    mWorkerServiceHandler.returnSpace(1L, WORKER_CAPACITY_BYTES);
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES));
  }
  
  @Test
  public void evictionTest() throws InvalidPathException, FileAlreadyExistException, IOException, 
      TException, FileDoesNotExistException {
    int fileId1 = createSimpleFile("/testFile1", OpType.WRITE_CACHE, (int) WORKER_CAPACITY_BYTES/3);
    Assert.assertTrue(fileId1 >= 0);
    ClientFileInfo fileInfo1 = mMasterInfo.getFileInfo("/testFile1");
    Assert.assertTrue(fileInfo1.isInMemory());
    int fileId2 = createSimpleFile("/testFile2", OpType.WRITE_CACHE, (int) WORKER_CAPACITY_BYTES/3);
    Assert.assertTrue(fileId2 >= 0);
    fileInfo1 = mMasterInfo.getFileInfo("/testFile1");
    ClientFileInfo fileInfo2 = mMasterInfo.getFileInfo("/testFile2");
    Assert.assertTrue(fileInfo1.isInMemory());
    Assert.assertTrue(fileInfo2.isInMemory());
    int fileId3 = createSimpleFile("/testFile3", OpType.WRITE_CACHE, (int) WORKER_CAPACITY_BYTES/2);
    fileInfo1 = mMasterInfo.getFileInfo("/testFile1");
    fileInfo2 = mMasterInfo.getFileInfo("/testFile2");
    ClientFileInfo fileInfo3 = mMasterInfo.getFileInfo("/testFile3");
    Assert.assertTrue(fileId3 >= 0);
    Assert.assertFalse(fileInfo1.isInMemory());
    Assert.assertTrue(fileInfo2.isInMemory());
    Assert.assertTrue(fileInfo3.isInMemory());
  }
}