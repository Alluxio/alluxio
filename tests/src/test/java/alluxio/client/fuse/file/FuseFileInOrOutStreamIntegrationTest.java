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

package alluxio.client.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.file.FuseFileStream;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Integration test for {@link alluxio.fuse.file.FuseFileInOrOutStream}.
 */
public class FuseFileInOrOutStreamIntegrationTest extends AbstractFuseFileStreamIntegrationTest {

  @Test
  public void createNonexistingClose() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    mStreamFactory.create(alluxioURI,
        OpenFlags.O_RDWR.intValue() | OpenFlags.O_CREAT.intValue(), MODE).close();
    URIStatus status = mFileSystem.getStatus(alluxioURI);
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isCompleted());
    Assert.assertEquals(0, status.getLength());
  }

  @Test
  public void createExistingClose() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE).close();
    URIStatus status = mFileSystem.getStatus(alluxioURI);
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isCompleted());
    Assert.assertEquals(DEFAULT_FILE_LEN, status.getLength());
  }

  @Test
  public void openTruncate() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    mFileSystem.createFile(alluxioURI).close();
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      stream.truncate(DEFAULT_FILE_LEN);
    }
    URIStatus status = mFileSystem.getStatus(alluxioURI);
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isCompleted());
    Assert.assertEquals(DEFAULT_FILE_LEN, status.getLength());
  }

  @Test
  public void createTruncateFlagClose() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    mStreamFactory.create(alluxioURI,
        OpenFlags.O_RDWR.intValue() | OpenFlags.O_TRUNC.intValue(), MODE).close();
    URIStatus status = mFileSystem.getStatus(alluxioURI);
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isCompleted());
    Assert.assertEquals(0, status.getLength());
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void createTruncateFlagRead() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream stream = mStreamFactory.create(alluxioURI,
        OpenFlags.O_RDWR.intValue() | OpenFlags.O_TRUNC.intValue(), MODE)) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
      stream.read(buffer, DEFAULT_FILE_LEN, 0);
    }
  }

  @Test
  public void createTruncateFlagWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    int newFileLength = 30;
    try (FuseFileStream stream = mStreamFactory.create(
        alluxioURI, OpenFlags.O_RDWR.intValue() | OpenFlags.O_TRUNC.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(0, newFileLength);
      stream.write(buffer, newFileLength, 0);
    }
    checkFileInAlluxio(alluxioURI, newFileLength, 0);
  }

  @Test
  public void createTruncateZeroWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    int newFileLength = 45;
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      Assert.assertEquals(DEFAULT_FILE_LEN, stream.getFileStatus().getFileLength());
      stream.truncate(0);
      Assert.assertEquals(0, stream.getFileStatus().getFileLength());
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(0, newFileLength);
      stream.write(buffer, newFileLength, 0);
      Assert.assertEquals(newFileLength, stream.getFileStatus().getFileLength());
    }
    checkFileInAlluxio(alluxioURI, newFileLength, 0);
  }

  @Test
  public void sequentialRead() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    URIStatus uriStatus = mFileSystem.getStatus(alluxioURI);
    try (FuseFileStream stream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      Assert.assertEquals(uriStatus.getLength(), stream.getFileStatus().getFileLength());
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN, stream.read(buffer, DEFAULT_FILE_LEN, 0));
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, DEFAULT_FILE_LEN, buffer));
      Assert.assertEquals(uriStatus.getLength(), stream.getFileStatus().getFileLength());
    }
  }

  @Test
  public void randomRead() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN / 2);
      Assert.assertEquals(DEFAULT_FILE_LEN / 2,
          stream.read(buffer, DEFAULT_FILE_LEN / 2, DEFAULT_FILE_LEN / 3));
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(
          DEFAULT_FILE_LEN / 3, DEFAULT_FILE_LEN / 2, buffer));
    }
  }

  @Test
  public void sequentialWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      stream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, stream.getFileStatus().getFileLength());
      buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      stream.write(buffer, DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, stream.getFileStatus().getFileLength());
    }
    checkFileInAlluxio(alluxioURI, DEFAULT_FILE_LEN * 2, 0);
  }

  @Test
  public void truncateZeroOrDefault() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      stream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, stream.getFileStatus().getFileLength());
      stream.truncate(0);
      Assert.assertEquals(0, stream.getFileStatus().getFileLength());
      buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN * 2);
      stream.write(buffer, DEFAULT_FILE_LEN * 2,  0);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, stream.getFileStatus().getFileLength());
      stream.truncate(DEFAULT_FILE_LEN * 2);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, stream.getFileStatus().getFileLength());
    }
    checkFileInAlluxio(alluxioURI, DEFAULT_FILE_LEN * 2, 0);
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void randomWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      stream.write(buffer, DEFAULT_FILE_LEN, 15);
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void readThenWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN, stream.read(buffer, DEFAULT_FILE_LEN, 0));
      stream.write(buffer, DEFAULT_FILE_LEN, 0);
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void writeThenRead() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      stream.write(buffer, DEFAULT_FILE_LEN, 0);
      stream.read(buffer, DEFAULT_FILE_LEN, 0);
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void readTruncateZero() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN, stream.read(buffer, DEFAULT_FILE_LEN, 0));
      stream.truncate(0);
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void truncateMiddle() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream stream
        = mStreamFactory.create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      stream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, stream.getFileStatus().getFileLength());
      stream.truncate(DEFAULT_FILE_LEN / 2);
    }
  }
}
