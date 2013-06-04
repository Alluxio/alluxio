package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.CommonUtils;
import tachyon.Constants;
import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;

/**
 * Unit tests on TachyonClient.
 */
public class TachyonFSTest {
  private final int WORKER_CAPACITY_BYTES = 20000;
  private final int USER_QUOTA_UNIT_BYTES = 1000;
  private final int WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS = 3;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mClient = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    System.setProperty("tachyon.worker.to.master.heartbeat.interval.ms",
        WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS + "");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.worker.to.master.heartbeat.interval.ms");
  }

  @Test
  public void createFileTest() throws Exception {
    int fileId = mClient.createFile("/root/testFile1");
    Assert.assertEquals(3, fileId);
    fileId = mClient.createFile("/root/testFile2");
    Assert.assertEquals(4, fileId);
    fileId = mClient.createFile("/root/testFile3");
    Assert.assertEquals(5, fileId);
  }

  @Test(expected = InvalidPathException.class)
  public void createFileWithInvalidPathExceptionTest() 
      throws InvalidPathException, FileAlreadyExistException {
    mClient.createFile("root/testFile1");
  }

  @Test(expected = FileAlreadyExistException.class)
  public void createFileWithFileAlreadyExistExceptionTest() 
      throws InvalidPathException, FileAlreadyExistException {
    int fileId = mClient.createFile("/root/testFile1");
    Assert.assertEquals(3, fileId);
    fileId = mClient.createFile("/root/testFile1");
  }

  @Test
  public void deleteFileTest() 
      throws InvalidPathException, FileAlreadyExistException, IOException, TException {
    List<ClientWorkerInfo> workers = mClient.getWorkersInfo();
    Assert.assertEquals(1, workers.size());
    Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
    Assert.assertEquals(0, workers.get(0).getUsedBytes());
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;

    for (int k = 0; k < 5; k ++) {
      int fileId = TestUtils.createSimpleByteFile(
          mClient, "/file" + k, OpType.WRITE_CACHE, writeBytes);
      TachyonFile file = mClient.getFile(fileId);
      Assert.assertTrue(file.isInMemory());
      Assert.assertTrue(mClient.exist("/file" + k));

      workers = mClient.getWorkersInfo();
      Assert.assertEquals(1, workers.size());
      Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
      Assert.assertEquals(writeBytes * (k + 1), workers.get(0).getUsedBytes());
    }

    for (int k = 0; k < 5; k ++) {
      int fileId = mClient.getFileId("/file" + k);
      mClient.delete(fileId);
      Assert.assertFalse(mClient.exist("/file" + k));

      CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS * 2 + 2);
      workers = mClient.getWorkersInfo();
      Assert.assertEquals(1, workers.size());
      Assert.assertEquals(WORKER_CAPACITY_BYTES, workers.get(0).getCapacityBytes());
      Assert.assertEquals(writeBytes * (4 - k), workers.get(0).getUsedBytes());
    }
  }

  @Test
  public void createRawTableTestEmptyMetadata()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException,
      TableDoesNotExistException, TException {
    int fileId = mClient.createRawTable("/tables/table1", 20);
    RawTable table = mClient.getRawTable(fileId);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());

    table = mClient.getRawTable("/tables/table1");
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(ByteBuffer.allocate(0), table.getMetadata());
  }

  @Test
  public void createRawTableTestWithMetadata()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException,
      TableDoesNotExistException, TException {
    int fileId = mClient.createRawTable("/tables/table1", 20, TestUtils.getIncreasingByteBuffer(9));
    RawTable table = mClient.getRawTable(fileId);
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(9), table.getMetadata());

    table = mClient.getRawTable("/tables/table1");
    Assert.assertEquals(fileId, table.getId());
    Assert.assertEquals("/tables/table1", table.getPath());
    Assert.assertEquals(20, table.getColumns());
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(9), table.getMetadata());
  }

  @Test(expected = InvalidPathException.class)
  public void createRawTableWithInvalidPathExceptionTest1()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException {
    mClient.createRawTable("tables/table1", 20);
  }

  @Test(expected = InvalidPathException.class)
  public void createRawTableWithInvalidPathExceptionTest2()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException, TableDoesNotExistException, TException {
    mClient.createRawTable("/tab les/table1", 20);
  }

  @Test(expected = FileAlreadyExistException.class)
  public void createRawTableWithFileAlreadyExistExceptionTest()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException {
    mClient.createRawTable("/table", 20);
    mClient.createRawTable("/table", 20);
  }

  @Test(expected = TableColumnException.class)
  public void createRawTableWithTableColumnExceptionTest1()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException {
    mClient.createRawTable("/table", Constants.MAX_COLUMNS);
  }

  @Test(expected = TableColumnException.class)
  public void createRawTableWithTableColumnExceptionTest2()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException {
    mClient.createRawTable("/table", 0);
  }

  @Test(expected = TableColumnException.class)
  public void createRawTableWithTableColumnExceptionTest3()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException {
    mClient.createRawTable("/table", -1);
  }

  @Test
  public void renameFileTest()
      throws InvalidPathException, FileAlreadyExistException {
    int fileId = mClient.createFile("/root/testFile1");
    mClient.rename("/root/testFile1", "/root/testFile2");
    Assert.assertEquals(fileId, mClient.getFileId("/root/testFile2"));
    Assert.assertFalse(mClient.exist("/root/testFile1"));
  }
}
