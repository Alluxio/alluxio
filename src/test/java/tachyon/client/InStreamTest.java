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

public class InStreamTest {
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

  /**
   * Create a simple file with length <code>len</code>.
   * @param len
   * @return file id of the new created file.
   * @throws FileAlreadyExistException 
   * @throws InvalidPathException 
   * @throws IOException 
   */
  private int createSimpleFile(String fileName, OpType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mClient.createFile(fileName);
    TachyonFile file = mClient.getFile(fileId);
    OutStream os = file.createOutStream(op);

    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();

    return fileId;
  }

  /**
   * Test <code>void read()</code>.
   */
  @Test
  public void readTest1() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 200; k += 33) {
      for (OpType op : OpType.values()) {
        if (op.isWrite()) {
          int fileId = createSimpleFile("/root/testFile_" + k + "_" + op, op, k);

          TachyonFile file = mClient.getFile(fileId);
          InStream is;
          if (k < 150) {
            is = file.createInStream(OpType.READ_TRY_CACHE);
          } else {
            is = file.createInStream(OpType.READ_NO_CACHE);
          }
          byte[] ret = new byte[k];
          int value = is.read();
          int cnt = 0;
          while (value != -1) {
            ret[cnt ++] = (byte) value;
            value = is.read();
          }
          Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
          is.close();
        }
      }
    }
  }

  /**
   * Test <code>void read(byte b[])</code>.
   */
  @Test
  public void readTest2() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 300; k += 33) {
      for (OpType op : OpType.values()) {
        if (op.isWrite()) {
          int fileId = createSimpleFile("/root/testFile_" + k + "_" + op, op, k);

          TachyonFile file = mClient.getFile(fileId);
          InStream is;
          if (k < 200) {
            is = file.createInStream(OpType.READ_TRY_CACHE);
          } else {
            is = file.createInStream(OpType.READ_NO_CACHE);
          }
          byte[] ret = new byte[k];
          Assert.assertEquals(k, is.read(ret));
          Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
          is.close();
        }
      }
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>.
   */
  @Test
  public void readTest3() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 300; k += 33) {
      for (OpType op : OpType.values()) {
        if (op.isWrite()) {
          int fileId = createSimpleFile("/root/testFile_" + k + "_" + op, op, k);

          TachyonFile file = mClient.getFile(fileId);
          InStream is;
          if (k < 200) {
            is = file.createInStream(OpType.READ_TRY_CACHE);
          } else {
            is = file.createInStream(OpType.READ_NO_CACHE);
          }
          byte[] ret = new byte[k / 2];
          Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
          Assert.assertTrue(TestUtils.equalIncreasingByteArray(k / 2, ret));
          is.close();

          if (k < 200) {
            is = file.createInStream(OpType.READ_TRY_CACHE);
          } else {
            // TODO Fix this.
            is = file.createInStream(OpType.READ_NO_CACHE);
          }
          ret = new byte[k];
          Assert.assertEquals(k, is.read(ret, 0, k));
          Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
          is.close();
        }
      }
    }
  }
}
