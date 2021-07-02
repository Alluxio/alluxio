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

package alluxio.client.fs.io;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.PreconditionMessage;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for {@link alluxio.client.block.LocalBlockInStream}.
 */
public final class LocalBlockInStreamIntegrationTest extends BaseIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private static FileSystem sFileSystem = null;
  private static CreateFilePOptions sWriteBoth;
  private static CreateFilePOptions sWriteAlluxio;
  private static OpenFilePOptions sReadNoCache;
  private static OpenFilePOptions sReadCachePromote;
  private static String sTestPath;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
    sWriteBoth = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
        .setRecursive(true).build();
    sWriteAlluxio = CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE)
        .setRecursive(true).build();
    sReadCachePromote =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    sReadNoCache = OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build();
    sTestPath = PathUtils.uniqPath();

    // Create files of varying size and write type to later read from
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());
        FileSystemTestUtils.createByteFile(sFileSystem, path, op, k);
      }
    }
  }

  private static List<CreateFilePOptions> getOptionSet() {
    List<CreateFilePOptions> ret = new ArrayList<>(2);
    ret.add(sWriteBoth);
    ret.add(sWriteAlluxio);
    return ret;
  }

  /**
   * Tests {@link alluxio.client.block.LocalBlockInStream#read()}.
   */
  @Test
  public void readTest1() throws Exception {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);
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

        is = sFileSystem.openFile(uri, sReadCachePromote);
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
        Assert.assertTrue(sFileSystem.getStatus(uri).getInAlluxioPercentage() == 100);
      }
    }
  }

  /**
   * Tests {@link alluxio.client.block.LocalBlockInStream#read(byte[])}.
   */
  @Test
  public void readTest2() throws Exception {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = sFileSystem.openFile(uri, sReadCachePromote);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sFileSystem.getStatus(uri).getInAlluxioPercentage() == 100);
      }
    }
  }

  /**
   * Tests {@link alluxio.client.block.LocalBlockInStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws Exception {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = sFileSystem.openFile(uri, sReadCachePromote);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sFileSystem.getStatus(uri).getInAlluxioPercentage() == 100);
      }
    }
  }

  /**
   * Tests {@link alluxio.client.block.LocalBlockInStream#seek(long)}. Validate the expected
   * exception for seeking a negative position.
   */
  @Test
  public void seekExceptionTest1() throws Exception {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), -1));
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        try (FileInStream is = sFileSystem.openFile(uri, sReadNoCache)) {
          is.seek(-1);
        }
      }
    }
  }

  /**
   * Tests {@link alluxio.client.block.LocalBlockInStream#seek(long)}. Validate the expected
   * exception for seeking a position that is past buffer limit.
   */
  @Test
  public void seekExceptionTest2() throws Exception {
    mThrown.expect(IllegalArgumentException.class);
    mThrown
        .expectMessage(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), 1));

    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        try (FileInStream is = sFileSystem.openFile(uri, sReadNoCache)) {
          is.seek(k + 1);
        }
      }
    }
  }

  /**
   * Tests {@link alluxio.client.block.LocalBlockInStream#seek(long)}.
   */
  @Test
  public void seek() throws Exception {
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);

        is.seek(k / 3);
        Assert.assertEquals(k / 3, is.read());
        is.seek(k / 2);
        Assert.assertEquals(k / 2, is.read());
        is.seek(k / 4);
        Assert.assertEquals(k / 4, is.read());
        is.close();
      }
    }
  }

  /**
   * Tests {@link alluxio.client.block.LocalBlockInStream#skip(long)}.
   */
  @Test
  public void skip() throws Exception {
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();

        is = sFileSystem.openFile(uri, sReadCachePromote);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
        Assert.assertTrue(sFileSystem.getStatus(uri).getInAlluxioPercentage() == 100);
      }
    }
  }
}
