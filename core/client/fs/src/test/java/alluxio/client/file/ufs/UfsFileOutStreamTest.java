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

package alluxio.client.file.ufs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.conf.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Add unit tests for {@link UfsFileOutStream}.
 */
public class UfsFileOutStreamTest {
  private static final int CHUNK_SIZE = 128;
  private String mRootUfs;
  private UnderFileSystem mUfs;

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    mRootUfs = AlluxioTestDirectory.createTemporaryDirectory("ufs").toString();
    UnderFileSystemFactoryRegistry.register(new LocalUnderFileSystemFactory());
    mUfs = UnderFileSystem.Factory.create(mRootUfs, Configuration.global());
  }

  @After
  public void after() throws IOException, AlluxioException {
    mUfs.deleteDirectory(mRootUfs, DeleteOptions.defaults().setRecursive(true));
  }

  @Test
  public void createClose() throws IOException {
    String ufsPath = getUfsPath();
    new UfsFileOutStream(mUfs.create(ufsPath)).close();
    assertTrue(mUfs.getStatus(ufsPath).isFile());
    assertEquals(0L, ((UfsFileStatus) mUfs.getStatus(ufsPath)).getContentLength());
  }

  @Test
  public void singleByteWrite() throws IOException {
    byte byteToWrite = 5;
    String ufsPath = getUfsPath();
    try (UfsFileOutStream outStream = new UfsFileOutStream(mUfs.create(ufsPath))) {
      outStream.write(byteToWrite);
    }
    try (InputStream inputStream = mUfs.open(ufsPath)) {
      assertEquals(byteToWrite, inputStream.read());
    }
  }

  @Test
  public void writeIncreasingBytes() throws IOException {
    String ufsPath = getUfsPath();
    try (UfsFileOutStream outStream = new UfsFileOutStream(mUfs.create(ufsPath))) {
      for (int i = 0; i < CHUNK_SIZE; i++) {
        outStream.write(i);
      }
    }
    verifyIncreasingBytesWritten(ufsPath, CHUNK_SIZE);
  }

  @Test
  public void writeIncreasingByteArray() throws IOException {
    String ufsPath = getUfsPath();
    try (UfsFileOutStream outStream = new UfsFileOutStream(mUfs.create(ufsPath))) {
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE));
    }
    verifyIncreasingBytesWritten(ufsPath, CHUNK_SIZE);
  }

  @Test
  public void writeIncreasingByteArrayOffsetLen() throws IOException {
    String ufsPath = getUfsPath();
    try (UfsFileOutStream outStream = new UfsFileOutStream(mUfs.create(ufsPath))) {
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE), 0, CHUNK_SIZE);
    }
    verifyIncreasingBytesWritten(ufsPath, CHUNK_SIZE);
  }

  @Test
  public void writePartialIncreasingByteArray() throws IOException {
    int offset = CHUNK_SIZE / 2;
    String ufsPath = getUfsPath();
    try (UfsFileOutStream outStream = new UfsFileOutStream(mUfs.create(ufsPath))) {
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE), offset, CHUNK_SIZE / 2);
    }
    verifyIncreasingBytesWritten(ufsPath, offset, CHUNK_SIZE / 2);
  }

  @Test
  public void writeOffset() throws IOException {
    int bytesToWrite = CHUNK_SIZE * 5 + CHUNK_SIZE / 2;
    int offset = CHUNK_SIZE / 3;
    String ufsPath = getUfsPath();
    try (UfsFileOutStream outStream = new UfsFileOutStream(mUfs.create(ufsPath))) {
      byte[] array = BufferUtils.getIncreasingByteArray(bytesToWrite + offset);
      outStream.write(array, offset, bytesToWrite);
    }
    verifyIncreasingBytesWritten(ufsPath, offset, bytesToWrite);
  }

  @Test
  public void writeWithNullUfsStream() {
    Assert.assertThrows(NullPointerException.class,
        () -> new UfsFileOutStream(null).close());
  }

  @Test
  public void writeOverflowOffLen() throws IOException {
    String ufsPath = getUfsPath();
    try (UfsFileOutStream outStream = new UfsFileOutStream(mUfs.create(ufsPath))) {
      assertThrows(IllegalArgumentException.class, () ->
          outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE), 5, CHUNK_SIZE + 5));
    }
  }

  @Test
  public void writeNullByteArray() throws IOException {
    String ufsPath = getUfsPath();
    try (UfsFileOutStream outStream = new UfsFileOutStream(mUfs.create(ufsPath))) {
      assertThrows(IllegalArgumentException.class, () ->
          outStream.write(null));
    }
  }

  @Test
  public void getBytesWrittenWhenWrite() throws IOException {
    String ufsPath = getUfsPath();
    try (UfsFileOutStream outStream = new UfsFileOutStream(mUfs.create(ufsPath))) {
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE), 0, CHUNK_SIZE);
      assertEquals(CHUNK_SIZE, outStream.getBytesWritten());
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE, CHUNK_SIZE), 0, CHUNK_SIZE);
      assertEquals(CHUNK_SIZE * 2, outStream.getBytesWritten());
    }
    verifyIncreasingBytesWritten(ufsPath, CHUNK_SIZE * 2);
  }

  private String getUfsPath() {
    return Paths.get(mRootUfs, String.valueOf(UUID.randomUUID())).toString();
  }

  private void verifyIncreasingBytesWritten(String ufs, int len) throws IOException {
    verifyIncreasingBytesWritten(ufs, 0, len);
  }

  private void verifyIncreasingBytesWritten(String ufs, int start, int len) throws IOException {
    int block = 128;
    byte[] array = new byte[Math.min(block, len)];
    int curToRead;
    try (InputStream inputStream = mUfs.open(ufs)) {
      while (len > 0) {
        curToRead = Math.min(len, block);
        int read = inputStream.read(array, 0, curToRead);
        Assert.assertTrue(read > 0);
        Assert.assertTrue(BufferUtils.matchIncreasingByteArray(start, 0, read, array));
        len -= read;
        start += read;
      }
    }
  }
}
