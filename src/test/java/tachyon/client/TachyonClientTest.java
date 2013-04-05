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
  LocalTachyonCluster ltc = null;
  TachyonClient client = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    ltc = new LocalTachyonCluster(5555, 6666, 1000);
    ltc.start();
    client = ltc.getClient();
  }

  @After
  public final void after() throws Exception {
    ltc.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void createFileTest() throws Exception {
    int fileId = client.createFile("/root/testFile1");
    Assert.assertEquals(3, fileId);
    fileId = client.createFile("/root/testFile2");
    Assert.assertEquals(4, fileId);
    fileId = client.createFile("/root/testFile3");
    Assert.assertEquals(5, fileId);
  }

  @Test(expected = InvalidPathException.class)
  public void createFileWithInvalidPathExceptionTest() 
      throws InvalidPathException, FileAlreadyExistException {
    client.createFile("root/testFile1");
  }

  @Test(expected = FileAlreadyExistException.class)
  public void createFileWithFileAlreadyExistExceptionTest() 
      throws InvalidPathException, FileAlreadyExistException {
    int fileId = client.createFile("/root/testFile1");
    Assert.assertEquals(3, fileId);
    fileId = client.createFile("/root/testFile1");
  }

  @Test
  public void deleteFileTest() 
      throws InvalidPathException, FileAlreadyExistException {
    int fileId = client.createFile("/root/testFile1");
    client.deleteFile(fileId);
    Assert.assertFalse(client.existFile("/root/testFile1"));
  }

  @Test
  public void renameFileTest()
      throws InvalidPathException, FileAlreadyExistException {
    int fileId = client.createFile("/root/testFile1");
    client.renameFile("/root/testFile1", "/root/testFile2");
    Assert.assertEquals(fileId, client.getFileId("/root/testFile2"));
    Assert.assertFalse(client.existFile("/root/testFile1"));
  }
}
