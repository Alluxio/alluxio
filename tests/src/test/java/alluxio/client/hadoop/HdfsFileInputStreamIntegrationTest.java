/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.hadoop;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.PreconditionMessage;
import alluxio.grpc.WritePType;
import alluxio.hadoop.HadoopClientTestUtils;
import alluxio.hadoop.HdfsFileInputStream;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;

import org.apache.hadoop.fs.Seekable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Integration tests for {@link HdfsFileInputStream}.
 */
public final class HdfsFileInputStreamIntegrationTest extends BaseIntegrationTest {

  private static final int FILE_LEN = 255;
  private static final int BUFFER_SIZE = 50;
  private static final String IN_MEMORY_FILE = "/inMemoryFile";
  private static final String UFS_ONLY_FILE = "/ufsOnlyFile";

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private FileSystem mFileSystem;
  private HdfsFileInputStream mInMemInputStream;
  private HdfsFileInputStream mUfsInputStream;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @After
  public final void after() throws IOException, AlluxioException {
    mInMemInputStream.close();
    mFileSystem.delete(new AlluxioURI(IN_MEMORY_FILE));
    if (mUfsInputStream != null) {
      mUfsInputStream.close();
      mFileSystem.delete(new AlluxioURI(UFS_ONLY_FILE));
    }
    HadoopClientTestUtils.disableMetrics(ServerConfiguration.global());
  }

  @Before
  public final void before() throws Exception {
    mFileSystem = sLocalAlluxioClusterResource.get().getClient();
    FileSystemTestUtils
        .createByteFile(mFileSystem, IN_MEMORY_FILE, WritePType.CACHE_THROUGH, FILE_LEN);
    mInMemInputStream = new HdfsFileInputStream(mFileSystem,
        new AlluxioURI(IN_MEMORY_FILE), null);
  }

  private void createUfsInStream(ReadType readType) throws Exception {
    InstancedConfiguration conf = new InstancedConfiguration(ServerConfiguration.copyProperties());
    conf.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, readType.name());
    FileSystem fs = FileSystem.Factory.create(conf);
    FileSystemTestUtils.createByteFile(fs, UFS_ONLY_FILE, WritePType.THROUGH,
        FILE_LEN);
    mUfsInputStream = new HdfsFileInputStream(fs, new AlluxioURI(UFS_ONLY_FILE), null);
  }

  /**
   * Tests {@link HdfsFileInputStream#available()}.
   */
  @Test
  public void available() throws Exception {
    Assert.assertEquals(FILE_LEN, mInMemInputStream.available());
    createUfsInStream(ReadType.NO_CACHE);
    Assert.assertEquals(FILE_LEN, mUfsInputStream.available());

    // Advance the streams and check available() again.
    byte[] buf = new byte[BUFFER_SIZE];
    int length1 = mInMemInputStream.read(buf);
    Assert.assertEquals(FILE_LEN - length1, mInMemInputStream.available());
    int length2 = mUfsInputStream.read(buf);
    Assert.assertEquals(FILE_LEN - length2, mUfsInputStream.available());
  }

  /**
   * Tests {@link HdfsFileInputStream#read()}.
   */
  @Test
  public void readTest1() throws Exception {
    createUfsInStream(ReadType.NO_CACHE);
    for (int i = 0; i < FILE_LEN; i++) {
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
   * Tests {@link HdfsFileInputStream#read(byte[])}.
   */
  @Test
  public void readTest2() throws Exception {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(buf);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));

    createUfsInStream(ReadType.NO_CACHE);
    Arrays.fill(buf, (byte) 0);
    length = mUfsInputStream.read(buf);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));

    Arrays.fill(buf, (byte) 0);
    length = mInMemInputStream.read(buf);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(buf);
    Assert.assertEquals(-1, length);
  }

  /**
   * Tests {@link HdfsFileInputStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws Exception {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));

    createUfsInStream(ReadType.NO_CACHE);
    Arrays.fill(buf, (byte) 0);
    length = mUfsInputStream.read(buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));

    Arrays.fill(buf, (byte) 0);
    length = mInMemInputStream.read(buf, 0, 1);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(buf, 0, 1);
    Assert.assertEquals(-1, length);
  }

  /**
   * Tests {@link HdfsFileInputStream#read(long, byte[], int, int)}.
   */
  @Test
  public void readTest4() throws Exception {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(0, buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    createUfsInStream(ReadType.NO_CACHE);
    Arrays.fill(buf, (byte) 0);
    length = mUfsInputStream.read(0, buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    buf = new byte[FILE_LEN - 10];
    Arrays.fill(buf, (byte) 0);
    length = mInMemInputStream.read(10, buf, 0, FILE_LEN - 10);
    Assert.assertEquals(FILE_LEN - 10, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    Arrays.fill(buf, (byte) 0);
    length = mUfsInputStream.read(10, buf, 0, FILE_LEN - 10);
    Assert.assertEquals(FILE_LEN - 10, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    Arrays.fill(buf, (byte) 0);
    length = mInMemInputStream.read(-1, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(-1, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);

    length = mInMemInputStream.read(FILE_LEN, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(FILE_LEN, buf, 0, FILE_LEN);
    Assert.assertEquals(-1, length);
  }

  /**
   * Tests {@link HdfsFileInputStream#read(java.nio.ByteBuffer buf)}.
   */
  @Test
  public void readTest5() throws Exception {
    byte[] data = new byte[FILE_LEN];
    ByteBuffer buf = ByteBuffer.wrap(data);
    int length = mInMemInputStream.read(buf);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, FILE_LEN, buf));

    createUfsInStream(ReadType.NO_CACHE);
    buf.rewind();
    buf.clear();
    length = mUfsInputStream.read(buf);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, FILE_LEN, buf));

    buf.rewind();
    buf.clear();
    length = mInMemInputStream.read(buf);
    Assert.assertEquals(-1, length);
    length = mUfsInputStream.read(buf);
    Assert.assertEquals(-1, length);
  }

  /**
   * Tests {@link HdfsFileInputStream#readFully(long, byte[])}.
   */
  @Test
  public void readFullyTest1() throws Exception {
    byte[] buf = new byte[FILE_LEN];
    mInMemInputStream.readFully(0, buf);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    createUfsInStream(ReadType.NO_CACHE);
    Arrays.fill(buf, (byte) 0);
    mUfsInputStream.readFully(0, buf);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    buf = new byte[FILE_LEN - 10];
    Arrays.fill(buf, (byte) 0);
    mInMemInputStream.readFully(10, buf);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    Arrays.fill(buf, (byte) 0);
    mUfsInputStream.readFully(10, buf);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    Arrays.fill(buf, (byte) 0);
    try {
      mInMemInputStream.readFully(-1, buf);
      Assert.fail("readFully() is expected to fail");
    } catch (EOFException e) {
      // this is expected
    }
    BufferUtils.equalConstantByteArray((byte) 0, FILE_LEN, buf);
    try {
      mUfsInputStream.readFully(-1, buf);
      Assert.fail("readFully() is expected to fail");
    } catch (EOFException e) {
      // this is expected
    }
    BufferUtils.equalConstantByteArray((byte) 0, FILE_LEN, buf);

    try {
      mInMemInputStream.readFully(FILE_LEN, buf);
      Assert.fail("readFully() is expected to fail");
    } catch (EOFException e) {
      // this is expected
    }
    BufferUtils.equalConstantByteArray((byte) 0, FILE_LEN, buf);
    try {
      mUfsInputStream.readFully(FILE_LEN, buf);
      Assert.fail("readFully() is expected to fail");
    } catch (EOFException e) {
      // this is expected
    }
    BufferUtils.equalConstantByteArray((byte) 0, FILE_LEN, buf);
  }

  /**
   * Tests {@link HdfsFileInputStream#readFully(long, byte[], int, int)}.
   */
  @Test
  public void readFullyTest2() throws Exception {
    byte[] buf = new byte[FILE_LEN];
    mInMemInputStream.readFully(0, buf, 0, FILE_LEN);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    createUfsInStream(ReadType.NO_CACHE);
    Arrays.fill(buf, (byte) 0);
    mUfsInputStream.readFully(0, buf, 0, FILE_LEN);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    buf = new byte[FILE_LEN - 10];
    Arrays.fill(buf, (byte) 0);
    mInMemInputStream.readFully(10, buf, 0, FILE_LEN - 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

    Arrays.fill(buf, (byte) 0);
    mUfsInputStream.readFully(10, buf, 0, FILE_LEN - 10);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(10, FILE_LEN - 10, buf));
    Assert.assertEquals(0, mUfsInputStream.getPos());

    Arrays.fill(buf, (byte) 0);
    try {
      mInMemInputStream.readFully(-1, buf, 0, FILE_LEN);
      Assert.fail("readFully() is expected to fail");
    } catch (EOFException e) {
      // this is expected
    }
    BufferUtils.equalConstantByteArray((byte) 0, FILE_LEN, buf);
    try {
      mUfsInputStream.readFully(-1, buf, 0, FILE_LEN);
      Assert.fail("readFully() is expected to fail");
    } catch (EOFException e) {
      // this is expected
    }
    BufferUtils.equalConstantByteArray((byte) 0, FILE_LEN, buf);

    try {
      mInMemInputStream.readFully(FILE_LEN, buf, 0, FILE_LEN);
      Assert.fail("readFully() is expected to fail");
    } catch (EOFException e) {
      // this is expected
    }
    BufferUtils.equalConstantByteArray((byte) 0, FILE_LEN, buf);
    try {
      mUfsInputStream.readFully(FILE_LEN, buf, 0, FILE_LEN);
      Assert.fail("readFully() is expected to fail");
    } catch (EOFException e) {
      // this is expected
    }
    BufferUtils.equalConstantByteArray((byte) 0, FILE_LEN, buf);
  }

  @Test
  public void inMemSeek() throws Exception {
    seekTest(mInMemInputStream);
  }

  @Test
  public void ufsSeek() throws Exception {
    createUfsInStream(ReadType.NO_CACHE);
    seekTest(mUfsInputStream);
  }

  private void seekTest(Seekable stream) throws Exception {
    stream.seek(0);
    Assert.assertEquals(0, stream.getPos());

    stream.seek(FILE_LEN / 2);
    Assert.assertEquals(FILE_LEN / 2, stream.getPos());

    stream.seek(1);
    Assert.assertEquals(1, stream.getPos());
  }

  @Test
  public void seekNegative() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), -1));
    mInMemInputStream.seek(-1);
  }

  @Test
  public void seekPastEof() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(),
        FILE_LEN + 1));
    mInMemInputStream.seek(FILE_LEN + 1);
  }

  @Test
  public void seekNegativeUfs() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), -1));
    createUfsInStream(ReadType.NO_CACHE);
    mUfsInputStream.seek(-1);
  }

  @Test
  public void seekPastEofUfs() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(),
        FILE_LEN + 1));
    createUfsInStream(ReadType.NO_CACHE);
    mUfsInputStream.seek(FILE_LEN + 1);
  }

  @Test
  public void positionedReadCache() throws Exception {
    createUfsInStream(ReadType.CACHE);
    mUfsInputStream.readFully(0, new byte[FILE_LEN]);
    FileSystemUtils.waitForAlluxioPercentage(mFileSystem, new AlluxioURI(UFS_ONLY_FILE), 100);
  }

  @Test
  public void positionedReadNoCache() throws Exception {
    createUfsInStream(ReadType.NO_CACHE);
    mUfsInputStream.readFully(0, new byte[FILE_LEN]);
    URIStatus statusUfsOnlyFile = mFileSystem.getStatus(new AlluxioURI(UFS_ONLY_FILE));
    Assert.assertEquals(0, statusUfsOnlyFile.getInAlluxioPercentage());
  }
}
