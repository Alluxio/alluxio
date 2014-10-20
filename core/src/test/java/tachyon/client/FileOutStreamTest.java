package tachyon.client;

import java.io.IOException;
import java.io.InputStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.UnderFileSystemCluster;
import tachyon.conf.UserConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Unit tests for <code>tachyon.client.FileOutStream</code>.
 */
public class FileOutStreamTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 32;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.user.default.block.size.byte");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    System.setProperty("tachyon.user.default.block.size.byte", "128");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  /**
   * Checks that we wrote the file correctly by reading it every possible way
   * 
   * @param filePath
   * @param byteArrayLimit
   * @throws IOException
   */
  private void checkWrite(TachyonURI filePath, WriteType op, int fileLen,
      int increasingByteArrayLen) throws IOException {
    for (ReadType rOp : ReadType.values()) {
      TachyonFile file = mTfs.getFile(filePath);
      InStream is = file.getInStream(rOp);
      Assert.assertEquals(fileLen, file.length());
      byte[] res = new byte[(int) file.length()];
      Assert.assertEquals((int) file.length(), is.read(res));
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
    }

    if (op.isThrough()) {
      TachyonFile file = mTfs.getFile(filePath);
      String checkpointPath = file.getUfsPath();
      UnderFileSystem ufs = UnderFileSystem.get(checkpointPath);

      InputStream is = ufs.open(checkpointPath);
      byte[] res = new byte[(int) file.length()];
      if (UnderFileSystemCluster.isUFSHDFS() && 0 == res.length) {
        // HDFS returns -1 for zero-sized byte array to indicate no more bytes available here.
        Assert.assertEquals(-1, is.read(res));
      } else {
        Assert.assertEquals((int) file.length(), is.read(res));
      }
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
    }
  }

  /**
   * Test <code>void write(int b)</code>.
   */
  @Test
  public void writeTest1() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        writeTest1Util(new TachyonURI("/root/testFile_" + k + "_" + op), op, k);
      }
    }
  }

  private void writeTest1Util(TachyonURI filePath, WriteType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mTfs.createFile(filePath);
    TachyonFile file = mTfs.getFile(fileId);
    OutStream os = file.getOutStream(op);
    Assert.assertTrue(os instanceof FileOutStream);
    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();
    checkWrite(filePath, op, len, len);
  }

  /**
   * Test <code>void write(byte[] b)</code>.
   */
  @Test
  public void writeTest2() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        writeTest2Util(new TachyonURI("/root/testFile_" + k + "_" + op), op, k);
      }
    }
  }

  private void writeTest2Util(TachyonURI filePath, WriteType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mTfs.createFile(filePath);
    TachyonFile file = mTfs.getFile(fileId);
    OutStream os = file.getOutStream(op);
    Assert.assertTrue(os instanceof FileOutStream);
    os.write(TestUtils.getIncreasingByteArray(len));
    os.close();
    checkWrite(filePath, op, len, len);
  }

  /**
   * Test <code>void write(byte[] b, int off, int len)</code>.
   */
  @Test
  public void writeTest3() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (WriteType op : WriteType.values()) {
        writeTest3Util(new TachyonURI("/root/testFile_" + k + "_" + op), op, k);
      }
    }
  }

  private void writeTest3Util(TachyonURI filePath, WriteType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = mTfs.createFile(filePath);
    TachyonFile file = mTfs.getFile(fileId);
    OutStream os = file.getOutStream(op);
    Assert.assertTrue(os instanceof FileOutStream);
    os.write(TestUtils.getIncreasingByteArray(0, len / 2), 0, len / 2);
    os.write(TestUtils.getIncreasingByteArray(len / 2, len / 2), 0, len / 2);
    os.close();
    checkWrite(filePath, op, len, len / 2 * 2);
  }

  /**
   * Test writing to a file for longer than HEARTBEAT_INTERVAL_MS to make sure the userId doesn't
   * change. Tracks [TACHYON-171].
   * 
   * @throws IOException
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws InterruptedException
   */
  @Test
  public void longWriteChangesUserId() throws IOException, InvalidPathException,
      FileAlreadyExistException, InterruptedException {
    TachyonURI filePath = new TachyonURI("/root/testFile");
    WriteType op = WriteType.THROUGH;
    int len = 2;
    int fileId = mTfs.createFile(filePath);
    long origId = mTfs.getUserId();
    TachyonFile file = mTfs.getFile(fileId);
    OutStream os = file.getOutStream(WriteType.THROUGH);
    Assert.assertTrue(os instanceof FileOutStream);
    os.write((byte) 0);
    Thread.sleep(UserConf.get().HEARTBEAT_INTERVAL_MS * 2);
    Assert.assertEquals(origId, mTfs.getUserId());
    os.write((byte) 1);
    os.close();
    checkWrite(filePath, op, len, len);
  }
}
