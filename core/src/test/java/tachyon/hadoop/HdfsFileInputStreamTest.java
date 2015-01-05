package tachyon.hadoop;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.ClientFileInfo;

public class HdfsFileInputStreamTest {
  private static final int USER_QUOTA_UNIT_BYTES = 100;
  private static final int WORKER_CAPACITY = 10000;
  private static final int FILE_LEN = 255;
  private static final int BUFFER_SIZE = 50;

  private static LocalTachyonCluster mLocalTachyonCluster = null;
  private static TachyonFS mTfs = null;
  private HdfsFileInputStream mInMemInputStream;
  private HdfsFileInputStream mUfsInputStream;

  @AfterClass
  public static final void afterClass() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @BeforeClass
  public static final void beforeClass() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes",  USER_QUOTA_UNIT_BYTES + "");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
    TestUtils.createByteFile(mTfs, "/testFile1", WriteType.CACHE_THROUGH, FILE_LEN);
    TestUtils.createByteFile(mTfs, "/testFile2", WriteType.THROUGH, FILE_LEN);
  }

  @After
  public final void after() throws Exception {
    mInMemInputStream.close();
    mUfsInputStream.close();
  }

  @Before
  public final void before() throws IOException {
    ClientFileInfo fileInfo = mTfs.getFileStatus(-1, new TachyonURI("/testFile1"));
    mInMemInputStream = new HdfsFileInputStream(mTfs, fileInfo.getId(),
        new Path(fileInfo.getUfsPath()), new Configuration(), BUFFER_SIZE);

    fileInfo = mTfs.getFileStatus(-1, new TachyonURI("/testFile2"));
    mUfsInputStream = new HdfsFileInputStream(mTfs, fileInfo.getId(),
        new Path(fileInfo.getUfsPath()), new Configuration(), BUFFER_SIZE);
  }

  /**
   * Test <code>int read()</code>.
   */
  @Test
  public void readTest1() throws IOException {
    for (int i = 0; i < FILE_LEN; i ++) {
      int value = mInMemInputStream.read();
      Assert.assertEquals(i & 0x00ff, value);
      value = mUfsInputStream.read();
      Assert.assertEquals(i & 0x00ff, value);
    }
    Assert.assertEquals(FILE_LEN, mInMemInputStream.getPos());
    Assert.assertEquals(FILE_LEN, mUfsInputStream.getPos());

    int value = mInMemInputStream.read();
    Assert.assertEquals(-1, value);
    value = mUfsInputStream.read();
    Assert.assertEquals(-1, value);
  }

  /**
   * Test <code>int read(byte[] b, int off, int len)</code>.
   */
  @Test
  public void readTest2() throws IOException {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(FILE_LEN, buf));

    Arrays.fill(buf, (byte)0);
    length = mUfsInputStream.read(buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(FILE_LEN, buf));

    Arrays.fill(buf, (byte)0);
    length = mInMemInputStream.read(buf, 0, 1);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(buf, 0, 1);
    Assert.assertEquals(-1, length);
  }

  /**
   * Test <code>int read(long position, byte[] buffer, int offset, int length)</code>.
   */
  @Test
  public void readTest3() throws IOException {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(0, buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    Arrays.fill(buf, (byte)0);
    length = mUfsInputStream.read(0, buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    Arrays.fill(buf, (byte)0);
    length = mInMemInputStream.read(10, buf, 0, FILE_LEN - 10);
    Assert.assertEquals(FILE_LEN - 10, length);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    Arrays.fill(buf, (byte)0);
    length = mUfsInputStream.read(10, buf, 0, FILE_LEN - 10);
    Assert.assertEquals(FILE_LEN - 10, length);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    Arrays.fill(buf, (byte)0);
    length = mInMemInputStream.read(-1, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(-1, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);

    length = mInMemInputStream.read(FILE_LEN, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(FILE_LEN, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);
  }

  @Test
  public void seekTest() throws IOException {
    mInMemInputStream.seek(0);
    Assert.assertEquals(0, mInMemInputStream.getPos());
    IllegalArgumentException exception = null;
    try {
      mInMemInputStream.seek(-1);
    } catch (IllegalArgumentException e) {
      exception = e;
    }
    Assert.assertEquals("Seek position is negative: -1", exception.getMessage());
    try {
      mInMemInputStream.seek(FILE_LEN + 1);
    } catch (IllegalArgumentException e) {
      exception = e;
    }
    Assert.assertEquals("Seek position is past EOF: " + (FILE_LEN + 1) + ", fileSize = " +
        FILE_LEN, exception.getMessage());

    mUfsInputStream.seek(0);
    Assert.assertEquals(0, mUfsInputStream.getPos());
    try {
      mUfsInputStream.seek(-1);
    } catch (IllegalArgumentException e) {
      exception = e;
    }
    Assert.assertEquals("Seek position is negative: -1", exception.getMessage());
    try {
      mUfsInputStream.seek(FILE_LEN + 1);
    } catch (IllegalArgumentException e) {
      exception = e;
    }
    Assert.assertEquals("Seek position is past EOF: " + (FILE_LEN + 1) + ", fileSize = " +
        FILE_LEN, exception.getMessage());
  }
}
