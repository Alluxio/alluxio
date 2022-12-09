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

package alluxio.fuse.ufs.stream;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.runtime.AlreadyExistsRuntimeException;
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.file.FuseFileStream;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.util.io.BufferUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * This class includes the tests for {@link alluxio.fuse.file.FuseFileOutStream}.
 */
public class OutStreamTest extends AbstractStreamTest {
  protected FuseFileStream createStream(AlluxioURI uri, boolean truncate) {
    int flags = OpenFlags.O_WRONLY.intValue();
    if (truncate) {
      flags |= OpenFlags.O_TRUNC.intValue();
    }
    return mStreamFactory
        .create(uri, flags, DEFAULT_MODE.toShort());
  }

  @Test
  public void createEmpty() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    createStream(alluxioURI, false).close();
    URIStatus status = mFileSystem.getStatus(alluxioURI);
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isCompleted());
    Assert.assertEquals(0, status.getLength());
  }

  @Test (expected = AlreadyExistsRuntimeException.class)
  public void createExisting() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put((byte) 'a');
      outStream.write(buffer, 1, 0);
    }
  }

  @Test
  public void createTruncateFlag() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    int newLen = 30;
    int newStartValue = 15;
    try (FuseFileStream outStream = createStream(alluxioURI, true)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(newStartValue, newLen);
      outStream.write(buffer, newLen, 0);
    }
    checkFile(alluxioURI, newLen, newStartValue);
  }

  @Test
  public void createTruncateZeroWrite() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    int newLen = 30;
    int newStartValue = 15;
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
      outStream.truncate(0);
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(newStartValue, newLen);
      outStream.write(buffer, newLen, 0);
    }
    checkFile(alluxioURI, newLen, newStartValue);
  }

  @Test (expected = FailedPreconditionRuntimeException.class)
  public void read() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      outStream.read(buffer, DEFAULT_FILE_LEN, 0);
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void randomWrite() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 15);
    }
  }

  @Test
  public void sequentialWriteAndgetFileLength() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
      buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileStatus().getFileLength());
    }
    checkFile(alluxioURI, DEFAULT_FILE_LEN * 2, 0);
  }

  @Test
  public void truncateZeroOrDefaultFileLen() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
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
    checkFile(alluxioURI, DEFAULT_FILE_LEN * 2, 0);
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void truncateMiddle() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileStatus().getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN / 2);
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void openExistingTruncateFuture() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
      // Alluxio does not support append to existing file
      outStream.truncate(DEFAULT_FILE_LEN * 2);
    }
  }

  @Test
  public void truncateBiggerThanBytesWritten() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
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
    checkFile(alluxioURI, DEFAULT_FILE_LEN * 3, 0);
  }

  @Test
  public void truncateFileLen() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
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
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
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
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
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
