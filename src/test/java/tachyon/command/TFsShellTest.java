package tachyon.command;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

/**
 * Unit tests on TFsShell.
 */
public class TFsShellTest {
  private final int mSizeBytes = Constants.MB * 10;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
  private TFsShell mFsShell = null;
  private ByteArrayOutputStream mOutput = null;
  private PrintStream mNewOutput = null;
  private PrintStream mOldOutput = null;

  private String getCommandOutput(String[] command) {
    String cmd = command[0];
    if (command.length == 2) {
      if (cmd.equals("ls")) {
        // Not sure how to handle this one.
        return null; 
      } else if (cmd.equals("mkdir")) {
        return "Successfully created directory " + command[1] + "\n";
      } else if (cmd.equals("rm")) {
        return command[1] + " has been removed" + "\n";
      }
    } else if (command.length == 3) {
      if (cmd.equals("mv")) {
        return "Renamed " + command[1] + " to " + command[2] + "\n";
      } else if (cmd.equals("copyFromLocal")) {
        return "Copied " + command[1] + " to " + command[2] + "\n";
      } else if (cmd.equals("copyToLocal")) {
        return "Copied " + command[1] + " to " + command[2] + "\n";
      }
    } else if (command.length > 3) {
      if (cmd.equals("location")) {
        StringBuilder ret = new StringBuilder();
        ret.append(command[1] + " with file id " + command[2] + " are on nodes: \n");
        for (int i = 3; i < command.length; i ++) {
          ret.append(command[i]+"\n");
        }
        return ret.toString();
      }
    }
    return null;
  }


  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(mSizeBytes);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
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
  public void mkdirTest() throws IOException {
    mFsShell.mkdir(new String[]{"mkdir", "tachyon://" + 
        InetAddress.getLocalHost().getCanonicalHostName() + ":" +
        mLocalTachyonCluster.getMasterPort() + "/root/testFile1"});
    TachyonFile tFile = mTfs.getFile("/root/testFile1");
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir", "/root/testFile1"}),
        mOutput.toString());
    Assert.assertTrue(tFile.isFolder());
  }

  @Test
  public void mkdirShortPathTest() throws IOException { 
    mFsShell.mkdir(new String[]{"mkdir", "/root/testFile1"});
    TachyonFile tFile = mTfs.getFile("/root/testFile1");
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir", "/root/testFile1"}),
        mOutput.toString());
    Assert.assertTrue(tFile.isFolder());
  }

  @Test
  public void mkdirComplexPathTest() throws IOException {
    mFsShell.mkdir(new String[]{"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"});
    TachyonFile tFile = mTfs.getFile("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File");
    Assert.assertNotNull(tFile);
    Assert.assertEquals(getCommandOutput(
        new String[] {"mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"}), mOutput.toString());
    Assert.assertTrue(tFile.isFolder());
  }

  @Test(expected = IOException.class)
  public void mkdirExistingTest() throws IOException {
    mFsShell.mkdir(new String[]{"mkdir", "/testFile1"});
    mFsShell.mkdir(new String[]{"mkdir", "/testFile1"});
  }

  @Test(expected = IOException.class)
  public void mkdirInvalidPathTest() throws IOException {
    mFsShell.mkdir(new String[]{"mkdir", "/test File Invalid Path"});  
  }

  @Test
  public void rmTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(getCommandOutput(
        new String[]{"mkdir", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertNotNull(mTfs.getFile("/testFolder1"));
    Assert.assertNotNull(mTfs.getFile("/testFolder1/testFolder2"));
    Assert.assertNotNull(mTfs.getFile("/testFolder1/testFolder2/testFile2"));
    mFsShell.rm(new String[]{"rm", "/testFolder1/testFolder2/testFile2"});
    toCompare.append(getCommandOutput(
        new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(mTfs.getFile("/testFolder1"));
    Assert.assertNotNull(mTfs.getFile("/testFolder1/testFolder2"));
    Assert.assertNull(mTfs.getFile("/testFolder1/testFolder2/testFile2"));
    mFsShell.rm(new String[]{"rm", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[]{"rm", "/testFolder1"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNull(mTfs.getFile("/testFolder1"));
    Assert.assertNull(mTfs.getFile("/testFolder1/testFolder2"));
    Assert.assertNull(mTfs.getFile("/testFolder1/testFolder2/testFile2"));
  }

  @Test
  public void rmNotExistingFileTest() throws IOException {
    Assert.assertEquals(0, mFsShell.rm(new String[]{"rm", "/testFile"}));
  }

  @Test
  public void renameTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[]{"mkdir", "/testFolder1"}));
    Assert.assertNotNull(mTfs.getFile("/testFolder1"));
    mFsShell.rename(new String[]{"rename", "/testFolder1", "/testFolder"});
    toCompare.append(getCommandOutput(new String[]{"mv", "/testFolder1", "/testFolder"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertNotNull(mTfs.getFile("/testFolder"));
    Assert.assertNull(mTfs.getFile("/testFolder1"));
  }

  @Test(expected = IOException.class)
  public void renameToExistingFileTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder"});
    toCompare.append(getCommandOutput(new String[]{"mkdir", "/testFolder"}));
    mFsShell.mkdir(new String[]{"mkdir", "/testFolder1"});
    toCompare.append(getCommandOutput(new String[]{"mkdir", "/testFolder1"}));
    mFsShell.rename(new String[]{"rename", "/testFolder1", "/testFolder"});
  }

  @Test
  public void renameParentDirectoryTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.mkdir(new String[]{"mkdir", "/test/File1"});
    toCompare.append(getCommandOutput(new String[]{"mkdir", "/test/File1"}));
    mFsShell.rename(new String[]{"rename", "/test", "/test2"});
    toCompare.append(getCommandOutput(new String[]{"mv", "/test", "/test2"}));
    Assert.assertNotNull(mTfs.getFile("/test2/File1"));
    Assert.assertNull(mTfs.getFile("/test"));
    Assert.assertNull(mTfs.getFile("/test/File1"));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
  }

  @Test
  public void copyFromLocalTest() throws IOException {
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte toWrite[] = TestUtils.getIncreasingByteArray(10);
    fos.write(toWrite);
    fos.close();
    mFsShell.copyFromLocal(new String[]{"copyFromLocal", testFile.getAbsolutePath(), "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[]{
        "copyFromLocal", testFile.getAbsolutePath(), "/testFile"}), mOutput.toString());
    TachyonFile tFile = mTfs.getFile("/testFile");
    Assert.assertNotNull(tFile);
    Assert.assertEquals(10, tFile.length());
    InStream tfis = tFile.getInStream(ReadType.NO_CACHE);
    byte read[] = new byte[10];
    tfis.read(read);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(10, read));
  }

  @Test
  public void copyFromLocalLargeTest() throws IOException {
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte toWrite[] = TestUtils.getIncreasingByteArray(mSizeBytes);
    fos.write(toWrite);
    fos.close();
    mFsShell.copyFromLocal(new String[]{"copyFromLocal", testFile.getAbsolutePath(), "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[]{
        "copyFromLocal", testFile.getAbsolutePath(), "/testFile"}), mOutput.toString());
    TachyonFile tFile = mTfs.getFile("/testFile");
    Assert.assertNotNull(tFile);
    Assert.assertEquals(mSizeBytes, tFile.length());
    InStream tfis = tFile.getInStream(ReadType.NO_CACHE);
    byte read[] = new byte[mSizeBytes];
    tfis.read(read);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(mSizeBytes, read));
  }

  @Test
  public void copyToLocalTest() throws IOException {
    TestUtils.createByteFile(mTfs, "/testFile", WriteType.CACHE, 10);
    mFsShell.copyToLocal(new String[]{
        "copyToLocal", "/testFile", mLocalTachyonCluster.getTachyonHome() + "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[]{"copyToLocal", "/testFile", 
        mLocalTachyonCluster.getTachyonHome() + "/testFile"}), mOutput.toString());
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    FileInputStream fis = new FileInputStream(testFile);
    byte read[] = new byte[10];
    fis.read(read);
    fis.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(10, read));
  }

  @Test
  public void copyToLocalLargeTest() throws IOException {
    TestUtils.createByteFile(mTfs, "/testFile", WriteType.CACHE, mSizeBytes);
    mFsShell.copyToLocal(new String[]{
        "copyToLocal", "/testFile", mLocalTachyonCluster.getTachyonHome() + "/testFile"});
    Assert.assertEquals(getCommandOutput(new String[]{"copyToLocal", "/testFile", 
        mLocalTachyonCluster.getTachyonHome() + "/testFile"}), mOutput.toString());
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    FileInputStream fis = new FileInputStream(testFile);
    byte read[] = new byte[mSizeBytes];
    fis.read(read);
    fis.close();
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(mSizeBytes, read));
  }
}
