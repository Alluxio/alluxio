package tachyon.command;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.thrift.TException;

import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;
import tachyon.client.TachyonClient;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests on TFsShell.
 */
public class TFsShellTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonClient mClient = null;
  private TFsShell mFsShell = null;
  private ByteArrayOutputStream mOutput = null;
  private PrintStream mNewOutput = null;
  private PrintStream mOldOutput = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(1000);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();
    mFsShell = new TFsShell();
    mOutput = new ByteArrayOutputStream();
    mNewOutput = new PrintStream(mOutput);
    mOldOutput = System.out;
    System.setOut(mNewOutput);
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.setOut(mOldOutput);
  }

  @Test
  public void mkdirTest() 
      throws InvalidPathException, FileAlreadyExistException, TException, UnknownHostException {
    mFsShell.mkdir(new String[]{"mkdir", "tachyon://" + 
        InetAddress.getLocalHost().getCanonicalHostName() + ":" +
        mLocalTachyonCluster.getMasterPort() + "/root/testFile1"});
    Assert.assertNotNull(mClient.getFile("/root/testFile1"));
    Assert.assertEquals(TestUtils.getCommandOutput(new String[] {"mkdir", "/root/testFile1"}),
        mOutput.toString());
  }

  @Test
  public void mkdirShortPathTest()
      throws InvalidPathException, FileAlreadyExistException, TException, UnknownHostException { 
    mFsShell.mkdir(new String[]{"mkdir", "/root/testFile1"});
    Assert.assertNotNull(mClient.getFile("/root/testFile1"));
    Assert.assertEquals(TestUtils.getCommandOutput(new String[] {"mkdir", "/root/testFile1"}),
        mOutput.toString());
  }

  @Test
  public void mkdirComplexPathTest()
      throws InvalidPathException, FileAlreadyExistException, TException, UnknownHostException {
    mFsShell.mkdir(new String[]{"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"});
    Assert.assertNotNull(mClient.getFile("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"));
    Assert.assertEquals(TestUtils.getCommandOutput(
        new String[] {"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"}), mOutput.toString());
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
      throws InvalidPathException, FileAlreadyExistException, TException, IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(TestUtils.getCommandOutput(
        new String[]{"mkdir", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertNotNull(mClient.getFile("/testFolder1"));
    Assert.assertNotNull(mClient.getFile("/testFolder1/testFolder2"));
    Assert.assertNotNull(mClient.getFile("/testFolder1/testFolder2/testFile2"));
    mFsShell.rm(new String[]{"rm", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(TestUtils.getCommandOutput(
        new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(mClient.getFile("/testFolder1"));
    Assert.assertNotNull(mClient.getFile("/testFolder1/testFolder2"));
    Assert.assertNull(mClient.getFile("/testFolder1/testFolder2/testFile2"));
    mFsShell.rm(new String[]{"rm", "/testFolder1"});
    toCompare.append(TestUtils.getCommandOutput(new String[]{"rm", "/testFolder1"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNull(mClient.getFile("/testFolder1"));
    Assert.assertNull(mClient.getFile("/testFolder1/testFolder2"));
    Assert.assertNull(mClient.getFile("/testFolder1/testFolder2/testFile2"));
  }

  @Test
  public void rmNotExistingFileTest()
      throws InvalidPathException, FileAlreadyExistException, UnknownHostException, TException {
    Assert.assertEquals(0, mFsShell.rm(new String[]{"rm", "/testFile"}));
  }

  @Test
  public void renameTest()
      throws InvalidPathException, FileAlreadyExistException, FileDoesNotExistException, 
      UnknownHostException, TException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder1"});
    toCompare.append(TestUtils.getCommandOutput(new String[]{"mkdir", "/testFolder1"}));
    Assert.assertNotNull(mClient.getFile("/testFolder1"));
    mFsShell.rename(new String[]{"rename", "/testFolder1", "/testFolder"});
    toCompare.append(TestUtils.getCommandOutput(new String[]{"mv", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(mClient.getFile("/testFolder"));
    Assert.assertNull(mClient.getFile("/testFolder1"));
  }

  @Test
  public void renameToExistingFileTest()
      throws InvalidPathException, FileAlreadyExistException, FileDoesNotExistException, 
      UnknownHostException, TException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder"});
    toCompare.append(TestUtils.getCommandOutput(new String[]{"mkdir", "/testFolder"}));
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder1"});
    toCompare.append(TestUtils.getCommandOutput(new String[]{"mkdir", "/testFolder1"}));
    Assert.assertEquals(-1, mFsShell.rename(new String[]{"rename", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }
  
  @Test
  public void renameParentDirectoryTest()
      throws InvalidPathException, FileAlreadyExistException, FileDoesNotExistException, 
      UnknownHostException, TException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[]{"mkdir", "/test/File1"});
    toCompare.append(TestUtils.getCommandOutput(new String[]{"mkdir", "/test/File1"}));
    mFsShell.rename(new String[]{"rename", "/test", "/test2"});
    toCompare.append(TestUtils.getCommandOutput(new String[]{"mv", "/test", "/test2"}));
    Assert.assertNotNull(mClient.getFile("/test2/File1"));
    Assert.assertNull(mClient.getFile("/test"));
    Assert.assertNull(mClient.getFile("/test/File1"));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }
}
