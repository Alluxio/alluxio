package tachyon.client;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests for <code>tachyon.client.OutStream</code>.
 */
public class OutStreamTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  private void writeTest1Util(String filePath, WriteType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mTfs.createFile(filePath);
    TachyonFile file = mTfs.getFile(fileId);
    OutStream os = file.getOutStream(op);
    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();

    file = mTfs.getFile(filePath);
    InStream is = file.getInStream(ReadType.NO_CACHE);
    byte[] res = new byte[(int) file.length()];
    is.read(res);
    boolean t = TestUtils.equalIncreasingByteArray(len, res);
    Assert.assertTrue(t);
//    file.releaseFileLock();
  }

  /**
   * Test <code>void write(int b)</code>.
   */
  @Test
  public void writeTest1() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 200; k += 33) {
      for (WriteType op : WriteType.values()) {
        writeTest1Util("/root/testFile_" + k + "_" + op, op, k);
      }
    }
  }

  private void writeTest2Util(String filePath, WriteType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mTfs.createFile(filePath);
    TachyonFile file = mTfs.getFile(fileId);
    OutStream os = file.getOutStream(op);

    os.write(TestUtils.getIncreasingByteArray(len));
    os.close();

    file = mTfs.getFile(filePath);
    InStream is = file.getInStream(ReadType.NO_CACHE);
    byte[] res = new byte[(int) file.length()];
    is.read(res);
    boolean t = TestUtils.equalIncreasingByteArray(len, res);
    Assert.assertTrue(t);
//    file.releaseFileLock();
  }

  /**
   * Test <code>void write(byte b[])</code>.
   */
  @Test
  public void writeTest2() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 200; k += 33) {
      for (WriteType op : WriteType.values()) {
        writeTest2Util("/root/testFile_" + k + "_" + op, op, k);
      }
    }
  }

  private void writeTest3Util(String filePath, WriteType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mTfs.createFile(filePath);
    TachyonFile file = mTfs.getFile(fileId);
    OutStream os = file.getOutStream(op);

    os.write(TestUtils.getIncreasingByteArray(len), 0, len / 2);
    os.close();

    file = mTfs.getFile(filePath);
    InStream is = file.getInStream(ReadType.NO_CACHE);
    byte[] res = new byte[(int) file.length()];
    is.read(res);
    boolean t = TestUtils.equalIncreasingByteArray(len / 2, res);
    Assert.assertTrue(t);
//    file.releaseFileLock();
  }

  /**
   * Test <code>void write(byte[] b, int off, int len)</code>.
   */
  @Test
  public void writeTest3() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 200; k += 33) {
      for (WriteType op : WriteType.values()) {
        writeTest3Util("/root/testFile_" + k + "_" + op, op, k);
      }
    }
  }
}
