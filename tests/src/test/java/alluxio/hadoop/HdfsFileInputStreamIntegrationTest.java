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

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.util.io.BufferUtils;

import org.apache.hadoop.conf.Configuration;
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
import java.util.Arrays;

/**
 * Integration tests for {@link HdfsFileInputStream}.
 */
public final class HdfsFileInputStreamIntegrationTest {
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
    ClientTestUtils.resetClient();
  }

  @Before
  public final void before() throws IOException {
    mFileSystem = sLocalAlluxioClusterResource.get().getClient();
    FileSystemTestUtils
        .createByteFile(mFileSystem, IN_MEMORY_FILE, WriteType.CACHE_THROUGH, FILE_LEN);
    mInMemInputStream = new HdfsFileInputStream(FileSystemContext.INSTANCE,
        new AlluxioURI(IN_MEMORY_FILE), new Configuration(), BUFFER_SIZE, null);
  }

  private void createUfsInStream(ReadType readType) throws IOException {
    String defaultReadType = alluxio.Configuration.get(PropertyKey.USER_FILE_READ_TYPE_DEFAULT);
    alluxio.Configuration.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, readType.name());
    FileSystemTestUtils.createByteFile(mFileSystem, UFS_ONLY_FILE, WriteType.THROUGH, FILE_LEN);
    mUfsInputStream = new HdfsFileInputStream(FileSystemContext.INSTANCE,
        new AlluxioURI(UFS_ONLY_FILE), new Configuration(), BUFFER_SIZE, null);
    alluxio.Configuration.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, defaultReadType);
  }

  private void createUfsInStreamNoPartialcache(ReadType readType) throws IOException {
    String defaultReadType = alluxio.Configuration.get(PropertyKey.USER_FILE_READ_TYPE_DEFAULT);
    String defaultPartialCache =
        alluxio.Configuration.get(PropertyKey.USER_FILE_CACHE_PARTIALLY_READ_BLOCK);
    alluxio.Configuration.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, readType.name());
    alluxio.Configuration.set(PropertyKey.USER_FILE_CACHE_PARTIALLY_READ_BLOCK, "false");
    FileSystemTestUtils.createByteFile(mFileSystem, UFS_ONLY_FILE, WriteType.THROUGH, FILE_LEN);
    mUfsInputStream = new HdfsFileInputStream(FileSystemContext.INSTANCE,
        new AlluxioURI(UFS_ONLY_FILE), new Configuration(), BUFFER_SIZE, null);
    alluxio.Configuration.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, defaultReadType);
    alluxio.Configuration
        .set(PropertyKey.USER_FILE_CACHE_PARTIALLY_READ_BLOCK, defaultPartialCache);
  }

  /**
   * Tests {@link HdfsFileInputStream#available()}.
   */
  @Test
  public void available() throws IOException {
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
  public void readTest1() throws IOException {
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
  public void readTest2() throws IOException {
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
  public void readTest3() throws IOException {
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
  public void readTest4() throws IOException {
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
   * Tests {@link HdfsFileInputStream#readFully(long, byte[])}.
   */
  @Test
  public void readFullyTest1() throws IOException {
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
  public void readFullyTest2() throws IOException {
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
  public void inMemSeek() throws IOException {
    seekTest(mInMemInputStream);
  }

  @Test
  public void ufsSeek() throws IOException {
    createUfsInStream(ReadType.NO_CACHE);
    seekTest(mUfsInputStream);
  }

  private void seekTest(Seekable stream) throws IOException {
    stream.seek(0);
    Assert.assertEquals(0, stream.getPos());

    stream.seek(FILE_LEN / 2);
    Assert.assertEquals(FILE_LEN / 2, stream.getPos());

    stream.seek(1);
    Assert.assertEquals(1, stream.getPos());
  }

  @Test
  public void seekNegative() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.SEEK_NEGATIVE.getMessage(-1));
    mInMemInputStream.seek(-1);
  }

  @Test
  public void seekPastEof() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.SEEK_PAST_EOF.getMessage(FILE_LEN + 1, FILE_LEN));
    mInMemInputStream.seek(FILE_LEN + 1);
  }

  @Test
  public void seekNegativeUfs() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.SEEK_NEGATIVE.getMessage(-1));
    createUfsInStream(ReadType.NO_CACHE);
    mUfsInputStream.seek(-1);
  }

  @Test
  public void seekPastEofUfs() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.SEEK_PAST_EOF.getMessage(FILE_LEN + 1, FILE_LEN));
    createUfsInStream(ReadType.NO_CACHE);
    mUfsInputStream.seek(FILE_LEN + 1);
  }

  @Test
  public void positionedReadCache() throws IOException, AlluxioException {
    createUfsInStream(ReadType.CACHE);
    mUfsInputStream.readFully(0, new byte[FILE_LEN]);
    URIStatus statusUfsOnlyFile = mFileSystem.getStatus(new AlluxioURI(UFS_ONLY_FILE));
    Assert.assertEquals(100, statusUfsOnlyFile.getInMemoryPercentage());
  }

  @Test
  public void positionedReadCacheNoPartialCache() throws IOException, AlluxioException {
    createUfsInStreamNoPartialcache(ReadType.CACHE);
    mUfsInputStream.readFully(0, new byte[FILE_LEN - 1]);
    URIStatus statusUfsOnlyFile = mFileSystem.getStatus(new AlluxioURI(UFS_ONLY_FILE));
    Assert.assertEquals(0, statusUfsOnlyFile.getInMemoryPercentage());
  }

  @Test
  public void positionedReadNoCache() throws IOException, AlluxioException {
    createUfsInStream(ReadType.NO_CACHE);
    mUfsInputStream.readFully(0, new byte[FILE_LEN]);
    URIStatus statusUfsOnlyFIle = mFileSystem.getStatus(new AlluxioURI(UFS_ONLY_FILE));
    Assert.assertEquals(0, statusUfsOnlyFIle.getInMemoryPercentage());
  }

  @Test
  public void positionedReadNoCacheNoPartialCache() throws IOException, AlluxioException {
    createUfsInStreamNoPartialcache(ReadType.NO_CACHE);
    mUfsInputStream.readFully(0, new byte[FILE_LEN]);
    URIStatus statusUfsOnlyFIle = mFileSystem.getStatus(new AlluxioURI(UFS_ONLY_FILE));
    Assert.assertEquals(0, statusUfsOnlyFIle.getInMemoryPercentage());
  }
}
