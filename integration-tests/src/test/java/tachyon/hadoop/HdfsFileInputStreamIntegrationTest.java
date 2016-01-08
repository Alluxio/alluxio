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

package tachyon.hadoop;

import java.io.IOException;
import java.util.Arrays;

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

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;
import tachyon.util.io.BufferUtils;

/**
 * Integration tests for {@link HdfsFileInputStream}.
 */
public class HdfsFileInputStreamIntegrationTest {
  private static final int USER_QUOTA_UNIT_BYTES = 100;
  private static final int WORKER_CAPACITY = 10 * Constants.MB;
  private static final int FILE_LEN = 255;
  private static final int BUFFER_SIZE = 50;

  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(WORKER_CAPACITY, USER_QUOTA_UNIT_BYTES, Constants.MB);
  private static TachyonFileSystem sTachyonFileSystem = null;
  private HdfsFileInputStream mInMemInputStream;
  private HdfsFileInputStream mUfsInputStream;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sTachyonFileSystem = sLocalTachyonClusterResource.get().getClient();
    TachyonFSTestUtils.createByteFile(sTachyonFileSystem, "/testFile1", TachyonStorageType.STORE,
        UnderStorageType.SYNC_PERSIST, FILE_LEN);
    TachyonFSTestUtils.createByteFile(sTachyonFileSystem, "/testFile2", TachyonStorageType.NO_STORE,
        UnderStorageType.SYNC_PERSIST, FILE_LEN);
  }

  @After
  public final void after() throws Exception {
    mInMemInputStream.close();
    mUfsInputStream.close();
  }

  @Before
  public final void before() throws IOException, TachyonException {
    TachyonFile file1 = sTachyonFileSystem.open(new TachyonURI("/testFile1"));
    FileInfo fileInfo1 = sTachyonFileSystem.getInfo(file1);
    mInMemInputStream = new HdfsFileInputStream(fileInfo1.getFileId(),
        new Path(fileInfo1.getUfsPath()), new Configuration(), BUFFER_SIZE, null);

    TachyonFile file2 = sTachyonFileSystem.open(new TachyonURI("/testFile2"));
    FileInfo fileInfo2 = sTachyonFileSystem.getInfo(file2);
    mUfsInputStream = new HdfsFileInputStream(fileInfo2.getFileId(),
        new Path(fileInfo2.getUfsPath()), new Configuration(), BUFFER_SIZE, null);
  }

  /**
   * Test {@link HdfsFileInputStream#read()}.
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
