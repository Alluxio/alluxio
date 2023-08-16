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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.FileSystemUtils;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.security.user.UserState;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Integration tests for reading from a remote worker.
 */
@DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "jiaming",
    comment = "fix tests here")
@Ignore
public class RemoteReadIntegrationTest extends BaseIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource;
  private FileSystem mFileSystem = null;
  private FileSystemContext mFsContext;
  private CreateFilePOptions mWriteAlluxio;
  private CreateFilePOptions mWriteUnderStore;
  private OpenFilePOptions mReadNoCache;
  private OpenFilePOptions mReadCache;

  /**
   * Constructor for {@link RemoteReadIntegrationTest}.
   */
  public RemoteReadIntegrationTest() {
    mLocalAlluxioClusterResource = new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.USER_UFS_BLOCK_READ_CONCURRENCY_MAX, 2)
        .build();
  }

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    UserState us = UserState.Factory.create(Configuration.global());
    mFsContext = FileSystemContext.create(us.getSubject(), Configuration.global());
    mWriteAlluxio = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
        .setRecursive(true).build();
    mWriteUnderStore = CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH)
        .setRecursive(true).build();
    mReadCache = OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mReadNoCache = OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build();
  }

  @After
  public void after() throws Exception {
    mFsContext.close();
  }

  /**
   * Tests the single byte read API from a remote location when the data is only in the
   * underlying storage.
   */
  @Test
  public void readTest1() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
      byte[] ret = new byte[k];
      int value = is.read();
      int cnt = 0;
      while (value != -1) {
        Assert.assertTrue(value >= 0);
        Assert.assertTrue(value < 256);
        ret[cnt++] = (byte) value;
        value = is.read();
      }
      Assert.assertEquals(cnt, k);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      if (k == 0) {
        FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, 100);
      } else {
        Assert.assertNotEquals(100, mFileSystem.getStatus(uri).getInAlluxioPercentage());
      }

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      value = is.read();
      cnt = 0;
      while (value != -1) {
        Assert.assertTrue(value >= 0);
        Assert.assertTrue(value < 256);
        ret[cnt++] = (byte) value;
        value = is.read();
      }
      Assert.assertEquals(cnt, k);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, 100);

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      value = is.read();
      cnt = 0;
      while (value != -1) {
        Assert.assertTrue(value >= 0);
        Assert.assertTrue(value < 256);
        ret[cnt++] = (byte) value;
        value = is.read();
      }
      Assert.assertEquals(cnt, k);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, 100);
    }
  }

  /**
   * Tests the batch read API from a remote location when the data is only in the underlying
   * storage.
   */
  @Test
  public void readTest2() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
      byte[] ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      if (k == 0) {
        FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, 100);
      } else {
        Assert.assertFalse(mFileSystem.getStatus(uri).getInAlluxioPercentage() == 100);
      }

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, 100);

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, 100);
    }
  }

  /**
   * Tests the batch read API with offset and length from a remote location when the data is only in
   * the underlying storage.
   */
  @Test
  public void readTest3() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
      byte[] ret = new byte[k / 2];
      Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
      is.close();
      if (k == 0) {
        FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, 100);
      } else {
        Assert.assertFalse(mFileSystem.getStatus(uri).getInAlluxioPercentage() == 100);
      }

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret, 0, k));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, 100);

      is = mFileSystem.openFile(uri, mReadCache);
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      is.close();
      FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, 100);
    }
  }

  /**
   * Tests the batch read API from a remote location when the data is only in the underlying
   * storage.
   */
  @Test
  public void readTest7() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
      byte[] ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
      Assert.assertEquals(-1, is.read(ret));
      is.close();
      Assert.assertFalse(mFileSystem.getStatus(uri).getInAlluxioPercentage() == 100);
    }
  }

  /**
   * Tests the read API from a remote location after a delay longer than the netty heartbeat
   * timeout.
   */
  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.WORKER_NETWORK_KEEPALIVE_TIME_MS, "1sec"})
  public void heartbeat1() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    int size = 100;
    AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + size);
    FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, size);

    FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
    CommonUtils.sleepMs(2000);
    byte[] ret = new byte[size];
    Assert.assertEquals(size, is.read(ret));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(size, ret));
    Assert.assertEquals(-1, is.read(ret));
    is.close();
    Assert.assertFalse(mFileSystem.getStatus(uri).getInAlluxioPercentage() == 100);
  }

  /**
   * Validates the expected exception for seeking a negative position.
   */
  @Test
  public void seekExceptionTest1() throws Exception {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), -1));
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      try (FileInStream is = mFileSystem.openFile(uri, mReadNoCache)) {
        is.seek(-1);
      }
    }
  }

  /**
   * Validates the expected exception for seeking a position that is past block size.
   */
  @Test
  public void seekExceptionTest2() throws Exception {
    mThrown.expect(IllegalArgumentException.class);
    mThrown
        .expectMessage(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), 1));
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      try (FileInStream is = mFileSystem.openFile(uri, mReadNoCache)) {
        is.seek(k + 1);
      }
    }
  }

  /**
   * Tests seeking through data which is in a remote location.
   */
  @Test
  public void seek() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadNoCache);

      Assert.assertEquals(0, is.read());
      is.seek(k / 3);
      Assert.assertEquals(k / 3, is.read());
      is.seek(k / 2);
      Assert.assertEquals(k / 2, is.read());
      is.seek(k / 4);
      Assert.assertEquals(k / 4, is.read());
      is.close();
    }
  }

  /**
   * Tests skipping through data which is in a remote location.
   */
  @Test
  public void skip() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      AlluxioURI uri = new AlluxioURI(uniqPath + "/file_" + k);
      FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, k);

      FileInStream is = mFileSystem.openFile(uri, mReadCache);
      Assert.assertEquals(k / 2, is.skip(k / 2));
      Assert.assertEquals(k / 2, is.read());
      is.close();
      IntegrationTestUtils.waitForFileCached(mFileSystem, uri, 1000);

      if (k >= 3) {
        is = mFileSystem.openFile(uri, mReadCache);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
        IntegrationTestUtils.waitForFileCached(mFileSystem, uri, 1000);
      }
    }
  }

  /**
   * Tests that reading a file the whole way through with the STORE ReadType will recache it.
   */
  @Test
  public void completeFileReadTriggersRecache() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    int len = 2;
    AlluxioURI uri = new AlluxioURI(uniqPath);
    FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, len);

    FileInStream is = mFileSystem.openFile(uri, mReadCache);
    for (int i = 0; i < len; ++i) {
      Assert.assertEquals(i, is.read());
    }
    is.close();
    IntegrationTestUtils.waitForFileCached(mFileSystem, uri, 1000);
  }

  /**
   * Tests that not reading a file the whole way through then closing it will cause it to not
   * recache.
   */
  @Test
  public void incompleteFileReadCancelsRecache() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    AlluxioURI uri = new AlluxioURI(uniqPath);
    FileSystemTestUtils.createByteFile(mFileSystem, uri, mWriteUnderStore, 2);

    FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
    Assert.assertEquals(0, is.read());
    is.close();
    Assert.assertFalse(mFileSystem.getStatus(uri).getInAlluxioPercentage() == 100);
    is = mFileSystem.openFile(uri, mReadNoCache);
    is.close();
  }

  /**
   * Tests that reading a file consisting of more than one block from the underfs works.
   */
  @Test
  public void readMultiBlockFile() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    int blockSizeByte = 10;
    int numBlocks = 10;
    AlluxioURI uri = new AlluxioURI(uniqPath);
    FileOutStream os = mFileSystem.createFile(uri, mWriteUnderStore);
    for (int i = 0; i < numBlocks; i++) {
      for (int j = 0; j < blockSizeByte; j++) {
        os.write((byte) (i * blockSizeByte + j));
      }
    }
    os.close();

    FileInStream is = mFileSystem.openFile(uri, mReadCache);
    for (int i = 0; i < blockSizeByte * numBlocks; i++) {
      Assert.assertEquals((byte) i, is.read());
    }
    is.close();
    IntegrationTestUtils.waitForFileCached(mFileSystem, uri, 1000);
  }

  /**
   * Tests that seeking around a file cached locally works.
   */
  @Test
  public void seekAroundLocalBlock() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    // The number of bytes per remote block read should be set to 100 in the before function
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath, 200, mWriteAlluxio);
    FileInStream is = mFileSystem.openFile(new AlluxioURI(uniqPath), mReadNoCache);
    Assert.assertEquals(0, is.read());
    is.seek(199);
    Assert.assertEquals(199, is.read());
    is.seek(99);
    Assert.assertEquals(99, is.read());
    is.close();
  }
}
