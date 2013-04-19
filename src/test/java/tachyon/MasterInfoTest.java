package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.SuspectedFileSizeException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for tachyon.MasterInfo
 */
public class MasterInfoTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(1000);
    mLocalTachyonCluster.start();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void addCheckpointTest() 
      throws FileDoesNotExistException, SuspectedFileSizeException, FileAlreadyExistException, 
      InvalidPathException {
    int fileId = mMasterInfo.createFile("/testFile", false);
    ClientFileInfo fileInfo = mMasterInfo.getFileInfo("/testFile");
    Assert.assertEquals("", fileInfo.getCheckpointPath());
    mMasterInfo.addCheckpoint(-1, fileId, 0, "/testPath");
    fileInfo = mMasterInfo.getFileInfo("/testFile");
    Assert.assertEquals("/testPath", fileInfo.getCheckpointPath());
    mMasterInfo.addCheckpoint(-1, fileId, 0, "/testPath2");
    fileInfo = mMasterInfo.getFileInfo("/testFile");
    Assert.assertEquals("/testPath", fileInfo.getCheckpointPath());
  }

  @Test(expected = FileDoesNotExistException.class)
  public void notFileCheckpointTest()
      throws FileDoesNotExistException, SuspectedFileSizeException, FileAlreadyExistException,
      InvalidPathException {
    int fileId = mMasterInfo.createFile("/testFile", true);
    mMasterInfo.addCheckpoint(-1, fileId, 0, "/testPath");    
  }  

  @Test
  public void createFileTest() 
      throws InvalidPathException, FileAlreadyExistException, FileDoesNotExistException {
    mMasterInfo.createFile("/testFile", false);
    Assert.assertFalse(mMasterInfo.getFileInfo("/testFile").isFolder());
  }

  @Test
  public void createDirectoryTest() 
      throws InvalidPathException, FileAlreadyExistException, FileDoesNotExistException {
    mMasterInfo.createFile("/testFolder", true);
    ClientFileInfo fileInfo = mMasterInfo.getFileInfo("/testFolder");
    Assert.assertTrue(fileInfo.isFolder());
  }

  @Test(expected = InvalidPathException.class)
  public void createFileInvalidPathTest() throws InvalidPathException, FileAlreadyExistException {
    mMasterInfo.createFile("testFile", false);
  }

  @Test(expected = FileAlreadyExistException.class)
  public void createAlreadyExistFileTest() throws InvalidPathException, FileAlreadyExistException {
    mMasterInfo.createFile("/testFile", false);
    mMasterInfo.createFile("/testFile", true);
  }

  @Test
  public void createRawTableTest()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException,
      FileDoesNotExistException {
    mMasterInfo.createRawTable("/testTable", 1, (ByteBuffer) null);
    Assert.assertTrue(mMasterInfo.getFileInfo("/testTable").isFolder());    
  }

  @Test(expected = TableColumnException.class)
  public void negativeColumnTest() 
      throws InvalidPathException, FileAlreadyExistException, TableColumnException {
    mMasterInfo.createRawTable("/testTable", -1, (ByteBuffer) null);
  }

  @Test(expected = TableColumnException.class)
  public void tooManyColumnsTest()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException {
    mMasterInfo.createRawTable("/testTable", Constants.MAX_COLUMNS + 1, (ByteBuffer) null);
  }

  @Test
  public void deleteFileTest() throws InvalidPathException, FileAlreadyExistException {
    int fileId = mMasterInfo.createFile("/testFile", false);
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFile"));
    mMasterInfo.delete(fileId);
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFile"));
  }

  @Test
  public void deleteEmptyDirectoryTest() throws InvalidPathException, FileAlreadyExistException {
    int fileId = mMasterInfo.createFile("/testFolder", true);
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder"));
    mMasterInfo.delete(fileId);
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder"));
  }

  @Test
  public void deleteDirectoryWithFilesTest() 
      throws InvalidPathException, FileAlreadyExistException {
    int folderId = mMasterInfo.createFile("/testFolder", true);
    int fileId = mMasterInfo.createFile("/testFolder/testFile", false);
    Assert.assertEquals(folderId, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    mMasterInfo.delete(folderId);
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFile"));
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest() 
      throws InvalidPathException, FileAlreadyExistException {
    int folderId = mMasterInfo.createFile("/testFolder", true);
    int folderId2 = mMasterInfo.createFile("/testFolder/testFolder2", true);
    int fileId = mMasterInfo.createFile("/testFolder/testFile", false);
    int fileId2 = mMasterInfo.createFile("/testFolder/testFolder2/testFile2", false);
    Assert.assertEquals(folderId, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(folderId2, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(fileId2, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
    mMasterInfo.delete(folderId);
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
  }

  @Test
  public void getCapacityBytesTest() {
    Assert.assertEquals(1000, mMasterInfo.getCapacityBytes());
  }

  @Test
  public void clientFileInfoEmptyFileTest() 
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistException {
    int fileId = mMasterInfo.createFile("/testFile", false);
    ClientFileInfo fileInfo = mMasterInfo.getFileInfo("/testFile");
    Assert.assertEquals("testFile", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getId());
    Assert.assertEquals(-1, fileInfo.getSizeBytes());
    Assert.assertEquals("", fileInfo.getCheckpointPath());
    Assert.assertFalse(fileInfo.isFolder());
    Assert.assertFalse(fileInfo.isNeedPin());
    Assert.assertTrue(fileInfo.isNeedCache());
    Assert.assertFalse(fileInfo.isReady());
  }

  @Test
  public void clientFileInfoDirectoryTest() 
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistException {
    int fileId = mMasterInfo.createFile("/testFolder", true);
    ClientFileInfo fileInfo = mMasterInfo.getFileInfo("/testFolder");
    Assert.assertEquals("testFolder", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getId());
    Assert.assertEquals(0, fileInfo.getSizeBytes());
    Assert.assertEquals("", fileInfo.getCheckpointPath());
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertFalse(fileInfo.isNeedPin());
    Assert.assertFalse(fileInfo.isNeedCache());
    Assert.assertTrue(fileInfo.isReady());
  }
}