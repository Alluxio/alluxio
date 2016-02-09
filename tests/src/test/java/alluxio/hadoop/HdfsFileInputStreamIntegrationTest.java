/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.util.io.BufferUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
    URIStatus fileStatus1 = sFileSystem.getStatus(file1);
    mInMemInputStream = new HdfsFileInputStream(file1, new Path(fileStatus1.getUfsPath()),
        new Configuration(), BUFFER_SIZE, null);

    AlluxioURI file2 = new AlluxioURI("/testFile2");
    URIStatus fileStatus2 = sFileSystem.getStatus(file2);
    mUfsInputStream = new HdfsFileInputStream(file2, new Path(fileStatus2.getUfsPath()),
        new Configuration(), BUFFER_SIZE, null);
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
   * Test {@link HdfsFileInputStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest2() throws IOException {
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
  public void readTest3() throws IOException {
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
