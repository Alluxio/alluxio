package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;

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
      InvalidPathException, BlockInfoException {
    int fileId = mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo("/testFile");
    Assert.assertEquals("", fileInfo.getCheckpointPath());
    mMasterInfo.addCheckpoint(-1, fileId, 1, "/testPath");
    fileInfo = mMasterInfo.getClientFileInfo("/testFile");
    Assert.assertEquals("/testPath", fileInfo.getCheckpointPath());
    mMasterInfo.addCheckpoint(-1, fileId, 1, "/testPath");
    fileInfo = mMasterInfo.getClientFileInfo("/testFile");
    Assert.assertEquals("/testPath", fileInfo.getCheckpointPath());
  }

  @Test(expected = FileDoesNotExistException.class)
  public void notFileCheckpointTest()
      throws FileDoesNotExistException, SuspectedFileSizeException, FileAlreadyExistException,
      InvalidPathException, BlockInfoException {
    int fileId = mMasterInfo.mkdir("/testFile");
    mMasterInfo.addCheckpoint(-1, fileId, 0, "/testPath");    
  }  

  @Test
  public void createFileTest() 
      throws InvalidPathException, FileAlreadyExistException, FileDoesNotExistException {
    mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertFalse(mMasterInfo.getClientFileInfo("/testFile").isFolder());
  }

  @Test
  public void createFilePerfTest()
      throws FileAlreadyExistException, InvalidPathException, FileDoesNotExistException {
    //    long sMs = System.currentTimeMillis();
    for (int k = 0; k < 200; k ++) {
      mMasterInfo.mkdir("/testFile" + Constants.PATH_SEPARATOR + MasterInfo.COL + k + "/" + 0);
    }
    //    System.out.println(System.currentTimeMillis() - sMs);
    //    sMs = System.currentTimeMillis();
    for (int k = 0; k < 200; k ++) {
      mMasterInfo.getClientFileInfo("/testFile" + Constants.PATH_SEPARATOR + 
          MasterInfo.COL + k + "/" + 0);
    }
    //    System.out.println(System.currentTimeMillis() - sMs);
  }

  @Test
  public void createDirectoryTest() 
      throws InvalidPathException, FileAlreadyExistException, FileDoesNotExistException {
    mMasterInfo.mkdir("/testFolder");
    ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo("/testFolder");
    Assert.assertTrue(fileInfo.isFolder());
  }

  @Test(expected = InvalidPathException.class)
  public void createFileInvalidPathTest() throws InvalidPathException, FileAlreadyExistException {
    mMasterInfo.createFile("testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
  }

  @Test(expected = FileAlreadyExistException.class)
  public void createAlreadyExistFileTest() throws InvalidPathException, FileAlreadyExistException {
    mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    mMasterInfo.mkdir("/testFile");
  }

  @Test
  public void createRawTableTest()
      throws InvalidPathException, FileAlreadyExistException, TableColumnException,
      FileDoesNotExistException {
    mMasterInfo.createRawTable("/testTable", 1, (ByteBuffer) null);
    Assert.assertTrue(mMasterInfo.getClientFileInfo("/testTable").isFolder());    
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
  public void deleteFileTest() 
      throws InvalidPathException, FileAlreadyExistException, TachyonException {
    int fileId = mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFile"));
    Assert.assertTrue(mMasterInfo.delete(fileId, true));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFile"));
  }

  @Test
  public void deleteEmptyDirectoryTest() 
      throws InvalidPathException, FileAlreadyExistException, TachyonException {
    int fileId = mMasterInfo.mkdir("/testFolder");
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder"));
    Assert.assertTrue(mMasterInfo.delete(fileId, true));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder"));
  }

  @Test
  public void deleteDirectoryWithFilesTest() 
      throws InvalidPathException, FileAlreadyExistException, TachyonException {
    int folderId = mMasterInfo.mkdir("/testFolder");
    int fileId = mMasterInfo.createFile("/testFolder/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(folderId, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertTrue(mMasterInfo.delete(folderId, true));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFile"));
  }

  @Test
  public void deleteDirectoryWithFilesTest2() 
      throws InvalidPathException, FileAlreadyExistException, TachyonException {
    int folderId = mMasterInfo.mkdir("/testFolder");
    int fileId = mMasterInfo.createFile("/testFolder/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(folderId, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertFalse(mMasterInfo.delete(folderId, false));
    Assert.assertEquals(folderId, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest() 
      throws InvalidPathException, FileAlreadyExistException, TachyonException {
    int folderId = mMasterInfo.mkdir("/testFolder");
    int folderId2 = mMasterInfo.mkdir("/testFolder/testFolder2");
    int fileId = mMasterInfo.createFile("/testFolder/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    int fileId2 = mMasterInfo.createFile(
        "/testFolder/testFolder2/testFile2", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(folderId, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(folderId2, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(fileId2, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
    Assert.assertTrue(mMasterInfo.delete(folderId, true));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest2() 
      throws InvalidPathException, FileAlreadyExistException, TachyonException {
    int folderId = mMasterInfo.mkdir("/testFolder");
    int folderId2 = mMasterInfo.mkdir("/testFolder/testFolder2");
    int fileId = mMasterInfo.createFile("/testFolder/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    int fileId2 = mMasterInfo.createFile(
        "/testFolder/testFolder2/testFile2", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(folderId, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(folderId2, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(fileId2, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
    Assert.assertFalse(mMasterInfo.delete(folderId, false));
    Assert.assertEquals(folderId, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(folderId2, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(fileId2, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
  }

  @Test
  public void getCapacityBytesTest() {
    Assert.assertEquals(1000, mMasterInfo.getCapacityBytes());
  }

  @Test
  public void clientFileInfoEmptyFileTest() 
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistException {
    int fileId = mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo("/testFile");
    Assert.assertEquals("testFile", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getId());
    Assert.assertEquals(-1, fileInfo.getLength());
    Assert.assertEquals("", fileInfo.getCheckpointPath());
    Assert.assertFalse(fileInfo.isFolder());
    Assert.assertFalse(fileInfo.isNeedPin());
    Assert.assertTrue(fileInfo.isNeedCache());
    Assert.assertFalse(fileInfo.isComplete());
  }

  @Test
  public void clientFileInfoDirectoryTest() 
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistException {
    int fileId = mMasterInfo.mkdir("/testFolder");
    ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo("/testFolder");
    Assert.assertEquals("testFolder", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertEquals("", fileInfo.getCheckpointPath());
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertFalse(fileInfo.isNeedPin());
    Assert.assertFalse(fileInfo.isNeedCache());
    Assert.assertTrue(fileInfo.isComplete());
  }
}