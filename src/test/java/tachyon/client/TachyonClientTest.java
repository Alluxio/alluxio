package tachyon.client;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.LocalTachyonCluster;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests on TachyonClient.
 */
public class TachyonClientTest {
  LocalTachyonCluster mLocalTachyonCluster = null;
  TachyonClient mClient = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(1000);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
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
      throws InvalidPathException, FileAlreadyExistException {
    int fileId = mClient.createFile("/root/testFile1");
    mClient.delete(fileId);
    Assert.assertFalse(mClient.exist("/root/testFile1"));
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
