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
import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.client.file.URIStatus;
import alluxio.exception.runtime.AlreadyExistsRuntimeException;
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.file.FuseFileStream;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Integration test for {@link alluxio.fuse.file.FuseFileOutStream}.
 */
public class FuseFileOutStreamIntegrationTest extends AbstractFuseFileStreamIntegrationTest {
  @Test
  public void createEmpty() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    mStreamFactory.create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE).close();
    URIStatus status = mFileSystem.getStatus(alluxioURI);
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isCompleted());
    Assert.assertEquals(0, status.getLength());
  }

  @Test (expected = AlreadyExistsRuntimeException.class)
  @DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "hua",
      comment = "fix the test case")
  @Ignore
  public void createExisting() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put((byte) 'a');
      outStream.write(buffer, 1, 0);
    }
  }

  @Test
  @DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "hua",
      comment = "fix the test case")
  @Ignore
  public void createTruncateFlag() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    int newLen = 30;
    int newStartValue = 15;
    try (FuseFileStream outStream = mStreamFactory.create(alluxioURI,
        OpenFlags.O_WRONLY.intValue() | OpenFlags.O_TRUNC.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(newStartValue, newLen);
      outStream.write(buffer, newLen, 0);
    }
    checkFileInAlluxio(alluxioURI, newLen, newStartValue);
  }

  @Test
  @DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "hua",
      comment = "fix the test case")
  @Ignore
  public void createTruncateZeroWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    int newLen = 30;
    int newStartValue = 15;
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      outStream.truncate(0);
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(newStartValue, newLen);
      outStream.write(buffer, newLen, 0);
    }
    checkFileInAlluxio(alluxioURI, newLen, newStartValue);
  }

  @Test (expected = FailedPreconditionRuntimeException.class)
  public void read() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
      outStream.read(buffer, DEFAULT_FILE_LEN, 0);
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void randomWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 15);
    }
  }

  @Test
  public void sequentialWriteAndgetFileLength() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
      buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileStatus().getFileLength());
    }
    checkFileInAlluxio(alluxioURI, DEFAULT_FILE_LEN * 2, 0);
  }

  @Test
  @DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "hua",
      comment = "fix the test case")
  @Ignore
  public void truncateZeroOrDefaultFileLen() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
      outStream.truncate(0);
      Assert.assertEquals(0, outStream.getFileStatus().getFileLength());
      buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN * 2);
      outStream.write(buffer, DEFAULT_FILE_LEN * 2,  0);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileStatus().getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN * 2);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileStatus().getFileLength());
    }
    checkFileInAlluxio(alluxioURI, DEFAULT_FILE_LEN * 2, 0);
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void truncateMiddle() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN / 2);
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  @DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "hua",
      comment = "fix the test case")
  @Ignore
  public void openExistingTruncateFuture() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      // Alluxio does not support append to existing file
      outStream.truncate(DEFAULT_FILE_LEN * 2);
    }
  }

  @Test
  public void truncateBiggerThanBytesWritten() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      Assert.assertEquals(0, outStream.getFileStatus().getFileLength());
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN * 2);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileStatus().getFileLength());
      buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN * 2);
      outStream.write(buffer, DEFAULT_FILE_LEN * 2, DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN * 3, outStream.getFileStatus().getFileLength());
    }
    checkFileInAlluxio(alluxioURI, DEFAULT_FILE_LEN * 3, 0);
  }

  @Test
  public void truncateFileLen() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      Assert.assertEquals(0, outStream.getFileStatus().getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
    }
    Assert.assertEquals(DEFAULT_FILE_LEN, mFileSystem.getStatus(alluxioURI).getLength());
  }

  @Test
  public void truncateBiggerThanFileLen() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      Assert.assertEquals(0, outStream.getFileStatus().getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN * 2);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileStatus().getFileLength());
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileStatus().getFileLength());
    }
    Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileSystem.getStatus(alluxioURI).getLength());
  }

  @Test
  public void truncateMultiple() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE)) {
      Assert.assertEquals(0, outStream.getFileStatus().getFileLength());
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN * 3);
      Assert.assertEquals(DEFAULT_FILE_LEN * 3, outStream.getFileStatus().getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
      outStream.truncate(0);
      Assert.assertEquals(0, outStream.getFileStatus().getFileLength());
    }
    Assert.assertEquals(0, mFileSystem.getStatus(alluxioURI).getLength());
  }
}
