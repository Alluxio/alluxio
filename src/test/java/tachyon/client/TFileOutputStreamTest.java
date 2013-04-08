package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

public class TFileOutputStreamTest {
  LocalTachyonCluster mLocalTachyonCluster = null;
  TachyonClient mClient = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mClient = mLocalTachyonCluster.getClient();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  private void writeTest1Util(String filePath, OpType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mClient.createFile(filePath);
    TachyonFile file = mClient.getFile(fileId);
    file.open(op);
    OutputStream os = file.getOutputStream();
    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();
    file.close();

    file = mClient.getFile(filePath);
    file.open(OpType.READ_TRY_CACHE);
    Assert.assertTrue(TestUtils.equalIncreasingByteBuffer(len, file.readByteBuffer()));
  }

  /**
   * Test <code>void write(int b)</code>.
   */
  @Test
  public void writeTest1() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 200; k += 33) {
      for (OpType op : OpType.values()) {
        if (op.isWrite()) {
          writeTest1Util("/root/testFile_" + k + "_" + op, op, k);
        }
      }
    }
  }

  private void writeTest2Util(String filePath, OpType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mClient.createFile(filePath);
    TachyonFile file = mClient.getFile(fileId);
    file.open(op);
    OutputStream os = file.getOutputStream();

    os.write(TestUtils.getIncreasingByteArray(len));
    os.close();
    file.close();

    file = mClient.getFile(filePath);
    file.open(OpType.READ_TRY_CACHE);
    Assert.assertTrue(TestUtils.equalIncreasingByteBuffer(len, file.readByteBuffer()));
  }

  /**
   * Test <code>void write(byte b[])</code>.
   */
  @Test
  public void writeTest2() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 200; k += 33) {
      for (OpType op : OpType.values()) {
        if (op.isWrite()) {
          writeTest2Util("/root/testFile_" + k + "_" + op, op, k);
        }
      }
    }
  }

  private void writeTest3Util(String filePath, OpType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mClient.createFile(filePath);
    TachyonFile file = mClient.getFile(fileId);
    file.open(op);
    OutputStream os = file.getOutputStream();

    os.write(TestUtils.getIncreasingByteArray(len), 0, len / 2);
    os.close();
    file.close();

    file = mClient.getFile(filePath);
    file.open(OpType.READ_TRY_CACHE);
    Assert.assertTrue(TestUtils.equalIncreasingByteBuffer(len / 2, file.readByteBuffer()));
  }
  /**
   * Test <code>void write(byte[] b, int off, int len)</code>.
   */
  @Test
  public void writeTest3() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 200; k += 33) {
      for (OpType op : OpType.values()) {
        if (op.isWrite()) {
          writeTest3Util("/root/testFile_" + k + "_" + op, op, k);
        }
      }
    }
  }
}
