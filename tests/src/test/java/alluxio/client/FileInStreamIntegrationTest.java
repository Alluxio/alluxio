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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for {@link alluxio.client.file.FileInStream}.
 */
public class FileInStreamIntegrationTest {
  private static final int BLOCK_SIZE = 30;
  private static final int MIN_LEN = BLOCK_SIZE + 1;
  private static final int MAX_LEN = BLOCK_SIZE * 4 + 1;
  private static final int DELTA = BLOCK_SIZE / 2;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(Constants.GB, BLOCK_SIZE);
  private static FileSystem sFileSystem = null;
  private static CreateFileOptions sWriteBoth;
  private static CreateFileOptions sWriteAlluxio;
  private static CreateFileOptions sWriteUnderStore;
  private static String sTestPath;

  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(60);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static final void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
    sWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough();
    sWriteAlluxio = StreamOptionUtils.getCreateFileOptionsMustCache();
    sWriteUnderStore = StreamOptionUtils.getCreateFileOptionsThrough();
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
    List<CreateFileOptions> ret = new ArrayList<CreateFileOptions>(3);
    ret.add(sWriteBoth);
    ret.add(sWriteAlluxio);
    ret.add(sWriteUnderStore);
    return ret;
  }

  /**
   * Tests {@link FileInStream#read()} across block boundary.
   */
  @Test
  public void readTest1() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = sTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
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

        is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
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
      }
    }
  }

  /**
   * Tests {@link FileInStream#read(byte[])}.
   */
  @Test
  public void readTest2() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = sTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Tests {@link FileInStream#read(byte[], int, int)}.
   */
  @Test
  public void readTest3() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = sTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Tests {@link FileInStream#read(byte[], int, int)} for end of file.
   */
  @Test
  public void readEndOfFileTest() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = sTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        try {
          byte[] ret = new byte[k / 2];
          int readBytes = is.read(ret, 0, k / 2);
          while (readBytes != -1) {
            readBytes = is.read(ret);
            Assert.assertTrue(0 != readBytes);
          }
          Assert.assertEquals(-1, readBytes);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Tests {@link FileInStream#seek(long)}. Validate the expected exception for seeking a negative
   * position.
   *
   * @throws IOException
   * @throws AlluxioException
   */
  @Test
  public void seekExceptionTest1() throws IOException, AlluxioException {
    mThrown.expect(IllegalArgumentException.class);
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = sTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        try {
          is.seek(-1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Tests {@link FileInStream#seek(long)}. Validate the expected exception for seeking a position
   * that is past EOF.
   *
   * @throws IOException
   */
  @Test
  public void seekExceptionTest2() throws IOException, AlluxioException {
    mThrown.expect(IllegalArgumentException.class);
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = sTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        try {
          is.seek(k + 1);
        } finally {
          is.close();
        }
      }
    }
  }

  /**
   * Tests {@link FileInStream#seek(long)}.
   *
   * @throws IOException
   * @throws AlluxioException
   */
  @Test
  public void seekTest() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = sTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
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
   * Tests {@link FileInStream#seek(long)} when at the end of a file at the block boundary.
   *
   * @throws IOException
   */
  @Test
  public void eofSeekTest() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    int length = BLOCK_SIZE * 3;
    for (CreateFileOptions op : getOptionSet()) {
      String filename = uniqPath + "/file_" + op.hashCode();
      AlluxioURI uri = new AlluxioURI(filename);
      FileSystemTestUtils.createByteFile(sFileSystem, filename, length, op);

      FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
      byte[] data = new byte[length];
      is.read(data, 0, length);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(length, data));
      is.seek(0);
      is.read(data, 0, length);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(length, data));
      is.close();
    }
  }

  /**
   * Tests {@link FileInStream#skip(long)}.
   */
  @Test
  public void skipTest() throws IOException, AlluxioException {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        String filename = sTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(k / 2, is.read());
        is.close();

        is = sFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        Assert.assertEquals(k / 3, is.skip(k / 3));
        Assert.assertEquals(k / 3, is.read());
        is.close();
      }
    }
  }
}
