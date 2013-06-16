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
 * Unit tests for <code>tachyon.client.InStream</code>.
 */
public class InStreamTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mClient = null;

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
   * Test <code>void read()</code>.
   */
  @Test
  public void readTest1() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 200; k += 33) {
      for (WriteType op : WriteType.values()) {
        int fileId = TestUtils.createByteFile(mClient, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mClient.getFile(fileId);
        InStream is;
        if (k < 150) {
          is = file.getInStream(ReadType.CACHE);
        } else {
          is = file.getInStream(ReadType.NO_CACHE);
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

  /**
   * Test <code>void read(byte b[])</code>.
   */
  @Test
  public void readTest2() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 300; k += 33) {
      for (WriteType op : WriteType.values()) {
        int fileId = TestUtils.createByteFile(mClient, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mClient.getFile(fileId);
        InStream is;
        if (k < 200) {
          is = file.getInStream(ReadType.CACHE);
        } else {
          is = file.getInStream(ReadType.NO_CACHE);
        }
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>.
   */
  @Test
  public void readTest3() throws IOException, InvalidPathException, FileAlreadyExistException {
    for (int k = 100; k <= 300; k += 33) {
      for (WriteType op : WriteType.values()) {
        int fileId = TestUtils.createByteFile(mClient, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mClient.getFile(fileId);
        InStream is;
        if (k < 200) {
          is = file.getInStream(ReadType.CACHE);
        } else {
          is = file.getInStream(ReadType.NO_CACHE);
        }
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = null;
        if (k < 200) {
          is = file.getInStream(ReadType.CACHE);
        } else {
          is = file.getInStream(ReadType.NO_CACHE);
        }
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }
}
