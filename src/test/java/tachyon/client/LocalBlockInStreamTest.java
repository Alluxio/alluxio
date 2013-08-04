package tachyon.client;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.LocalTachyonCluster;
import tachyon.TestUtils;

public class LocalBlockInStreamTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
  private Set<WriteType> mWriteCacheType;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();

    mWriteCacheType = new HashSet<WriteType>();
    mWriteCacheType.add(WriteType.CACHE);
    mWriteCacheType.add(WriteType.CACHE_THROUGH);
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
  public void readTest1() throws IOException {
    for (int k = 100; k <= 100; k += 33) {
      for (WriteType op : mWriteCacheType) {
        int fileId = TestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mTfs.getFile(fileId);
        InStream is = file.getInStream(ReadType.NO_CACHE);
        Assert.assertTrue(is instanceof LocalBlockInStream);
        byte[] ret = new byte[k];
        int value = is.read();
        int cnt = 0;
        while (value != -1) {
          ret[cnt ++] = (byte) value;
          value = is.read();
        }
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(file.isInMemory());

        is = file.getInStream(ReadType.CACHE);
        Assert.assertTrue(is instanceof LocalBlockInStream);
        ret = new byte[k];
        value = is.read();
        cnt = 0;
        while (value != -1) {
          ret[cnt ++] = (byte) value;
          value = is.read();
        }
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(file.isInMemory());
      }
    }
  }

  /**
   * Test <code>void read(byte b[])</code>.
   */
  @Test
  public void readTest2() throws IOException {
    for (int k = 100; k <= 300; k += 33) {
      for (WriteType op : mWriteCacheType) {
        int fileId = TestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mTfs.getFile(fileId);
        InStream is = file.getInStream(ReadType.NO_CACHE);
        Assert.assertTrue(is instanceof LocalBlockInStream);
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(file.isInMemory());

        is = file.getInStream(ReadType.CACHE);
        Assert.assertTrue(is instanceof LocalBlockInStream);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(file.isInMemory());
      }
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>.
   */
  @Test
  public void readTest3() throws IOException {
    for (int k = 100; k <= 300; k += 33) {
      for (WriteType op : mWriteCacheType) {
        int fileId = TestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mTfs.getFile(fileId);
        InStream is = file.getInStream(ReadType.NO_CACHE);
        Assert.assertTrue(is instanceof LocalBlockInStream);
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();
        Assert.assertTrue(file.isInMemory());

        is = file.getInStream(ReadType.CACHE);
        Assert.assertTrue(is instanceof LocalBlockInStream);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(file.isInMemory());
      }
    }
  }

  /**
   * Test <code>long skip(long len)</code>.
   */
  @Test
  public void skipTest() throws IOException {
    for (int k = 10; k <= 200; k += 33) {
      for (WriteType op : mWriteCacheType) {
        int fileId = TestUtils.createByteFile(mTfs, "/root/testFile_" + k + "_" + op, op, k);

        TachyonFile file = mTfs.getFile(fileId);
        InStream is = file.getInStream(ReadType.CACHE);
        Assert.assertTrue(is instanceof LocalBlockInStream);
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();
        Assert.assertTrue(file.isInMemory());

        is = file.getInStream(ReadType.CACHE);
        Assert.assertTrue(is instanceof LocalBlockInStream);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals((byte) (2 * t + 1), is.read());
        is.close();
        Assert.assertTrue(file.isInMemory());
      }
    }
  }
}
