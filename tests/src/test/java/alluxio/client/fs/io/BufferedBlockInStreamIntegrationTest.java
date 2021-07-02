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
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for {@link BlockInStream}.
 */
public final class BufferedBlockInStreamIntegrationTest extends BaseIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private static FileSystem sFileSystem;
  private static String sTestPath = PathUtils.uniqPath();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();

    // Create files of varying size and write type to later read from
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());
        FileSystemTestUtils.createByteFile(sFileSystem, path, op, k);
      }
    }
  }

  private static List<CreateFilePOptions> getOptionSet() {
    List<CreateFilePOptions> ret = new ArrayList<>(3);
    CreateFilePOptions optionsDefault = CreateFilePOptions.getDefaultInstance();
    ret.add(optionsDefault.toBuilder().setWriteType(WritePType.CACHE_THROUGH)
        .setRecursive(true).build());
    ret.add(optionsDefault.toBuilder().setWriteType(WritePType.MUST_CACHE).setRecursive(true)
        .build());
    ret.add(optionsDefault.toBuilder().setWriteType(WritePType.THROUGH).setRecursive(true)
        .build());
    return ret;
  }

  /**
   * Tests {@link alluxio.client.block.BufferedBlockInStream#read()}.
   */
  @Test
  public void readTest1() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());
        for (int i = 0; i < 2; i++) {
          FileInStream is = sFileSystem.openFile(path, FileSystemTestUtils.toOpenFileOptions(op));
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
        }
      }
    }
  }

  /**
   * Tests {@link alluxio.client.block.BufferedBlockInStream#read(byte[])}.
   */
  @Test
  public void readTest2() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(path, FileSystemTestUtils.toOpenFileOptions(op));
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = sFileSystem.openFile(path, FileSystemTestUtils.toOpenFileOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Tests {@link alluxio.client.block.BufferedBlockInStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(path, FileSystemTestUtils.toOpenFileOptions(op));

        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = sFileSystem.openFile(path, FileSystemTestUtils.toOpenFileOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Tests {@link alluxio.client.block.BufferedBlockInStream#skip(long)}.
   */
  @Test
  public void skip() throws IOException, AlluxioException {
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(path, FileSystemTestUtils.toOpenFileOptions(op));

        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();

        is = sFileSystem.openFile(path, FileSystemTestUtils.toOpenFileOptions(op));
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
      }
    }
  }
}
