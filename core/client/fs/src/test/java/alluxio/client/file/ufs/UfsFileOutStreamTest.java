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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DeletePOptions;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Add unit tests for {@link UfsFileOutStream}.
 */
@RunWith(Parameterized.class)
public class UfsFileOutStreamTest extends AbstractUfsStreamTest {
  /**
   * Runs {@link UfsFileInStreamTest} with different configuration combinations.
   *
   * @param localDataCacheEnabled whether local data cache is enabled
   */
  public UfsFileOutStreamTest(boolean localDataCacheEnabled) {
    super(localDataCacheEnabled);
  }

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    String ufs = AlluxioTestDirectory.createTemporaryDirectory("ufsInStream").toString();
    mRootUfs = new AlluxioURI(ufs);
    UnderFileSystemFactoryRegistry.register(new LocalUnderFileSystemFactory());
    mFileSystem = FileSystem.Factory.create(FileSystemContext.create(
        ClientContext.create(mConf)), FileSystemOptions.create(mConf,
        Optional.of(new UfsFileSystemOptions(ufs))));
  }

  @After
  public void after() throws IOException, AlluxioException {
    for (URIStatus status : mFileSystem.listStatus(mRootUfs)) {
      mFileSystem.delete(new AlluxioURI(status.getUfsPath()),
          DeletePOptions.newBuilder().setRecursive(true).build());
    }
  }

  @Test
  public void createClose() throws IOException, AlluxioException {
    AlluxioURI ufsPath = getUfsPath();
    mFileSystem.createFile(ufsPath).close();
    assertFalse(mFileSystem.getStatus(ufsPath).isFolder());
    assertEquals(0L, mFileSystem.getStatus(ufsPath).getLength());
  }

  @Test
  public void singleByteWrite() throws IOException, AlluxioException {
    byte byteToWrite = 5;
    AlluxioURI ufsPath = getUfsPath();
    try (FileOutStream outStream = mFileSystem.createFile(ufsPath)) {
      outStream.write(byteToWrite);
    }
    try (InputStream inputStream = mFileSystem.openFile(ufsPath)) {
      assertEquals(byteToWrite, inputStream.read());
    }
  }

  @Test
  public void writeIncreasingBytes() throws IOException, AlluxioException {
    AlluxioURI ufsPath = getUfsPath();
    try (FileOutStream outStream = mFileSystem.createFile(ufsPath)) {
      for (int i = 0; i < CHUNK_SIZE; i++) {
        outStream.write(i);
      }
    }
    verifyIncreasingBytesWritten(ufsPath, CHUNK_SIZE);
  }

  @Test
  public void writeIncreasingByteArray() throws IOException, AlluxioException {
    AlluxioURI ufsPath = getUfsPath();
    try (FileOutStream outStream = mFileSystem.createFile(ufsPath)) {
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE));
    }
    verifyIncreasingBytesWritten(ufsPath, CHUNK_SIZE);
  }

  @Test
  public void writeIncreasingByteArrayOffsetLen() throws IOException, AlluxioException {
    AlluxioURI ufsPath = getUfsPath();
    try (FileOutStream outStream = mFileSystem.createFile(ufsPath)) {
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE), 0, CHUNK_SIZE);
    }
    verifyIncreasingBytesWritten(ufsPath, CHUNK_SIZE);
  }

  @Test
  public void writePartialIncreasingByteArray() throws IOException, AlluxioException {
    int offset = CHUNK_SIZE / 2;
    AlluxioURI ufsPath = getUfsPath();
    try (FileOutStream outStream = mFileSystem.createFile(ufsPath)) {
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE), offset, CHUNK_SIZE / 2);
    }
    verifyIncreasingBytesWritten(ufsPath, offset, CHUNK_SIZE / 2);
  }

  @Test
  public void writeOffset() throws IOException, AlluxioException {
    int bytesToWrite = CHUNK_SIZE * 5 + CHUNK_SIZE / 2;
    int offset = CHUNK_SIZE / 3;
    AlluxioURI ufsPath = getUfsPath();
    try (FileOutStream outStream = mFileSystem.createFile(ufsPath)) {
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
  public void writeOverflowOffLen() throws IOException, AlluxioException {
    AlluxioURI ufsPath = getUfsPath();
    try (FileOutStream outStream = mFileSystem.createFile(ufsPath)) {
      assertThrows(IllegalArgumentException.class, () ->
          outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE), 5, CHUNK_SIZE + 5));
    }
  }

  @Test
  public void writeNullByteArray() throws IOException, AlluxioException {
    AlluxioURI ufsPath = getUfsPath();
    try (FileOutStream outStream = mFileSystem.createFile(ufsPath)) {
      assertThrows(IllegalArgumentException.class, () ->
          outStream.write(null));
    }
  }

  @Test
  public void getBytesWrittenWhenWrite() throws IOException, AlluxioException {
    AlluxioURI ufsPath = getUfsPath();
    try (FileOutStream outStream = mFileSystem.createFile(ufsPath)) {
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE), 0, CHUNK_SIZE);
      assertEquals(CHUNK_SIZE, outStream.getBytesWritten());
      outStream.write(BufferUtils.getIncreasingByteArray(CHUNK_SIZE, CHUNK_SIZE), 0, CHUNK_SIZE);
      assertEquals(CHUNK_SIZE * 2, outStream.getBytesWritten());
    }
    verifyIncreasingBytesWritten(ufsPath, CHUNK_SIZE * 2);
  }

  private void verifyIncreasingBytesWritten(AlluxioURI ufsPath, int len)
      throws IOException, AlluxioException {
    verifyIncreasingBytesWritten(ufsPath, 0, len);
  }

  private void verifyIncreasingBytesWritten(AlluxioURI ufsPath, int start, int len)
      throws IOException, AlluxioException {
    int block = 128;
    byte[] array = new byte[Math.min(block, len)];
    int curToRead;
    try (InputStream inputStream = mFileSystem.openFile(ufsPath)) {
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
