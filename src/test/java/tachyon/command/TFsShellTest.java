package tachyon.command;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.thrift.TException;

import tachyon.LocalTachyonCluster;
import tachyon.client.TachyonClient;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests on TFsShell.
 */
public class TFsShellTest {
  LocalTachyonCluster mLocalTachyonCluster = null;
  TachyonClient mClient = null;
  TFsShell mFsShell = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(1000);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();
    mFsShell = new TFsShell();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Test
  public void mkdirTest() 
      throws InvalidPathException, FileAlreadyExistException, TException, UnknownHostException {
    mFsShell.mkdir(new String[]{"mkdir", "tachyon://" + 
        InetAddress.getLocalHost().getCanonicalHostName() + ":" +
        mLocalTachyonCluster.getMasterPort() + "/root/testFile1"});
    Assert.assertTrue(mClient.getFile("/root/testFile1") != null);
  }

  @Test
  public void mkdirShortPathTest()
      throws InvalidPathException, FileAlreadyExistException, TException, UnknownHostException { 
    mFsShell.mkdir(new String[]{"mkdir", "/root/testFile1"});
    Assert.assertTrue(mClient.getFile("/root/testFile1") != null);
  }

  @Test
  public void mkdirComplexPathTest()
      throws InvalidPathException, FileAlreadyExistException, TException, UnknownHostException {
    mFsShell.mkdir(new String[]{"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"});
    Assert.assertTrue(mClient.getFile("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File") != null);
  }

  @Test(expected = FileAlreadyExistException.class)
  public void mkdirExistingTest()
      throws InvalidPathException, FileAlreadyExistException, TException, UnknownHostException {
    mFsShell.mkdir(new String[]{"mkdir", "/testFile1"});
    mFsShell.mkdir(new String[]{"mkdir", "/testFile1"});
  }

  @Test(expected = InvalidPathException.class)
  public void mkdirInvalidPathTest()
      throws InvalidPathException, FileAlreadyExistException, TException, UnknownHostException {
    mFsShell.mkdir(new String[]{"mkdir", "/test File Invalid Path"});  
  }

  @Test
  public void rmTest() 
      throws InvalidPathException, FileAlreadyExistException, UnknownHostException, TException {
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder1/testFolder2/testFile2"});
    Assert.assertTrue(mClient.getFile("/testFolder1") != null);
    Assert.assertTrue(mClient.getFile("/testFolder1/testFolder2") != null);
    Assert.assertTrue(mClient.getFile("/testFolder1/testFolder2/testFile2") != null);
    mFsShell.rm(new String[]{"rm", "/testFolder1/testFolder2/testFile2"});
    Assert.assertTrue(mClient.getFile("/testFolder1") != null);
    Assert.assertTrue(mClient.getFile("/testFolder1/testFolder2") != null);
    Assert.assertEquals(null, mClient.getFile("/testFolder1/testFolder2/testFile2"));
    mFsShell.rm(new String[]{"rm", "/testFolder1"});
    Assert.assertEquals(null, mClient.getFile("/testFolder1"));
    Assert.assertEquals(null, mClient.getFile("/testFolder1/testFolder2"));
    Assert.assertEquals(null, mClient.getFile("/testFolder1/testFolder2/testFile2"));
  }

  @Test @Ignore
  /**
   * Seems there is a bug, deleting fileId of -1 does not throw file does not exist exception in 
   * MasterServiceHandler
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws UnknownHostException
   * @throws TException
   */
  public void rmNotExistingFileTest()
      throws InvalidPathException, FileAlreadyExistException, UnknownHostException, TException {
    Assert.assertEquals(-1, mFsShell.rm(new String[]{"rm", "/testFile"}));
  }

  @Test
  public void renameTest()
      throws InvalidPathException, FileAlreadyExistException, FileDoesNotExistException, 
      UnknownHostException, TException {
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder1"});
    Assert.assertTrue(mClient.getFile("/testFolder1") != null);
    mFsShell.rename(new String[]{"rename", "/testFolder1", "/testFolder2"});
    Assert.assertTrue(mClient.getFile("/testFolder2") != null);
    Assert.assertEquals(null, mClient.getFile("/testFolder1"));
  }

  @Test
  public void renameToExistingFileTest()
      throws InvalidPathException, FileAlreadyExistException, FileDoesNotExistException, 
      UnknownHostException, TException {
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder"});
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder1"});
    Assert.assertEquals(-1, mFsShell.rename(new String[]{"rename", "/testFolder1", "/testFolder"}));
  }
}
