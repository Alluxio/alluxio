/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.util.io.BufferUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Seekable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();
  private static FileSystem sFileSystem = null;
  private HdfsFileInputStream mInMemInputStream;
  private HdfsFileInputStream mUfsInputStream;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testFile1", WriteType.CACHE_THROUGH, FILE_LEN);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile2", WriteType.THROUGH, FILE_LEN);
  }

  @After
  public final void after() throws Exception {
    mInMemInputStream.close();
    mUfsInputStream.close();
  }

  @Before
  public final void before() throws IOException, AlluxioException {
    AlluxioURI file1 = new AlluxioURI("/testFile1");
    mInMemInputStream = new HdfsFileInputStream(file1, new Configuration(), BUFFER_SIZE, null);

    AlluxioURI file2 = new AlluxioURI("/testFile2");
    mUfsInputStream = new HdfsFileInputStream(file2, new Configuration(), BUFFER_SIZE, null);
  }

  /**
   * Test {@link HdfsFileInputStream#read()}.
   */
  @Test
  public void readTest1() throws IOException {
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
   * Test {@link HdfsFileInputStream#read(byte[])}.
   */
  @Test
  public void readTest2() throws IOException {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(buf);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));

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
   * Test {@link HdfsFileInputStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws IOException {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));

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
   * Test {@link HdfsFileInputStream#read(long, byte[], int, int)}.
   */
  @Test
  public void readTest4() throws IOException {
    byte[] buf = new byte[FILE_LEN];
    int length = mInMemInputStream.read(0, buf, 0, FILE_LEN);
    Assert.assertEquals(FILE_LEN, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

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
   * Test {@link HdfsFileInputStream#readFully(long, byte[])}.
   */
  @Test
  public void readFullyTest1() throws IOException {
    byte[] buf = new byte[FILE_LEN];
    mInMemInputStream.readFully(0, buf);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

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
   * Test {@link HdfsFileInputStream#readFully(long, byte[], int, int)}.
   */
  @Test
  public void readFullyTest2() throws IOException {
    byte[] buf = new byte[FILE_LEN];
    mInMemInputStream.readFully(0, buf, 0, FILE_LEN);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buf));
    Assert.assertEquals(0, mInMemInputStream.getPos());

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
  public void inMemSeekTest() throws IOException {
    seekTest(mInMemInputStream);
  }

  @Test
  public void ufsSeekTest() throws IOException {
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
  public void seekNegativeTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.SEEK_NEGATIVE.getMessage(-1));
    mInMemInputStream.seek(-1);
  }

  @Test
  public void seekPastEofTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.SEEK_PAST_EOF.getMessage(FILE_LEN + 1, FILE_LEN));
    mInMemInputStream.seek(FILE_LEN + 1);
  }

  @Test
  public void seekNegativeUfsTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.SEEK_NEGATIVE.getMessage(-1));
    mUfsInputStream.seek(-1);
  }

  @Test
  public void seekPastEofUfsTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.SEEK_PAST_EOF.getMessage(FILE_LEN + 1, FILE_LEN));
    mUfsInputStream.seek(FILE_LEN + 1);
  }
}
