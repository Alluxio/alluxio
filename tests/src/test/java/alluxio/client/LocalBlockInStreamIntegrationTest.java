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

package alluxio.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import alluxio.LocalAlluxioClusterResource;
import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.Configuration;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.AlluxioException;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

/**
 * Integration tests for {@link alluxio.client.block.LocalBlockInStream}.
 */
public final class LocalBlockInStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();
  private static FileSystem sFileSystem = null;
  private static CreateFileOptions sWriteBoth;
  private static CreateFileOptions sWriteAlluxio;
  private static OpenFileOptions sReadNoCache;
  private static OpenFileOptions sReadCache;
  private static Configuration sConfiguration;
  private static String sTestPath;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
    sConfiguration = sLocalAlluxioClusterResource.get().getMasterConf();
    sWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough(sConfiguration);
    sWriteAlluxio = StreamOptionUtils.getCreateFileOptionsMustCache(sConfiguration);
    sReadCache = StreamOptionUtils.getOpenFileOptionsCache(sConfiguration);
    sReadNoCache = StreamOptionUtils.getOpenFileOptionsNoCache(sConfiguration);
    sTestPath = PathUtils.uniqPath();

    // Create files of varying size and write type to later read from
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        AlluxioURI path = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());
        FileSystemTestUtils.createByteFile(sFileSystem, path, op, k);
      }
    }
  }

  private static List<CreateFileOptions> getOptionSet() {
    List<CreateFileOptions> ret = new ArrayList<CreateFileOptions>(2);
    ret.add(sWriteBoth);
    ret.add(sWriteAlluxio);
    return ret;
  }

  /**
   * Test {@link alluxio.client.block.LocalBlockInStream#read()}.
   */
  @Test
  public void readTest1() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
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

        is = sFileSystem.openFile(uri, sReadCache);
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
        Assert.assertTrue(sFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Test {@link alluxio.client.block.LocalBlockInStream#read(byte[])}.
   */
  @Test
  public void readTest2() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = sFileSystem.openFile(uri, sReadCache);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Test {@link alluxio.client.block.LocalBlockInStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = sFileSystem.openFile(uri, sReadCache);
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
        Assert.assertTrue(sFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      }
    }
  }

  /**
   * Test {@link alluxio.client.block.LocalBlockInStream#seek(long)}. Validate the expected
   * exception for seeking a negative position.
   *
   * @throws IOException
   * @throws AlluxioException
   */
  @Test
  public void seekExceptionTest1() throws IOException, AlluxioException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE, -1));
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);

        try {
          is.seek(-1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Test {@link alluxio.client.block.LocalBlockInStream#seek(long)}. Validate the expected
   * exception for seeking a position that is past buffer limit.
   *
   * @throws IOException
   * @throws AlluxioException
   */
  @Test
  public void seekExceptionTest2() throws IOException, AlluxioException {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE, 1));

    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);
        try {
          is.seek(k + 1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Test {@link alluxio.client.block.LocalBlockInStream#seek(long)}.
   *
   * @throws IOException
   * @throws AlluxioException
   */
  @Test
  public void seekTest() throws IOException, AlluxioException {
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
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
   * Test {@link alluxio.client.block.LocalBlockInStream#skip(long)}.
   */
  @Test
  public void skipTest() throws IOException, AlluxioException {
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        AlluxioURI uri = new AlluxioURI(sTestPath + "/file_" + k + "_" + op.hashCode());

        FileInStream is = sFileSystem.openFile(uri, sReadNoCache);
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();

        is = sFileSystem.openFile(uri, sReadCache);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
        Assert.assertTrue(sFileSystem.getStatus(uri).getInMemoryPercentage() == 100);
      }
    }
  }
}
