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
import alluxio.fuse.file.FuseFileOutStream;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Integration test for {@link alluxio.fuse.file.FuseFileOutStream}.
 */
public class FuseFileOutStreamIntegrationTest extends AbstractFuseFileStreamIntegrationTest {
  @Test
  public void createEmpty() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    FuseFileOutStream.create(mFileSystem, mAuthPolicy, alluxioURI,
        OpenFlags.O_WRONLY.intValue(), MODE, Optional.empty()).close();
    URIStatus status = mFileSystem.getStatus(alluxioURI);
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isCompleted());
    Assert.assertEquals(0, status.getLength());
  }

  @Test (expected = IOException.class)
  public void createExisting() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    URIStatus uriStatus = mFileSystem.getStatus(alluxioURI);
    try (FuseFileOutStream outStream = FuseFileOutStream
        .create(mFileSystem, mAuthPolicy, alluxioURI,
        OpenFlags.O_WRONLY.intValue(), MODE, Optional.of(uriStatus))) {
      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put((byte) 'a');
      outStream.write(buffer, 1, 0);
    }
  }

  @Test
  public void createTruncateFlag() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    URIStatus uriStatus = mFileSystem.getStatus(alluxioURI);
    int newLen = 30;
    int newStartValue = 15;
    try (FuseFileOutStream outStream = FuseFileOutStream
        .create(mFileSystem, mAuthPolicy, alluxioURI,
        OpenFlags.O_WRONLY.intValue() | OpenFlags.O_TRUNC.intValue(),
            MODE, Optional.of(uriStatus))) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(newStartValue, newLen);
      outStream.write(buffer, newLen, 0);
    }
    checkFileInAlluxio(alluxioURI, newLen, newStartValue);
  }

  @Test
  public void createTruncateZeroWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    URIStatus uriStatus = mFileSystem.getStatus(alluxioURI);
    int newLen = 30;
    int newStartValue = 15;
    try (FuseFileOutStream outStream = FuseFileOutStream.create(mFileSystem, mAuthPolicy,
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE, Optional.of(uriStatus))) {
      outStream.truncate(0);
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(newStartValue, newLen);
      outStream.write(buffer, newLen, 0);
    }
    checkFileInAlluxio(alluxioURI, newLen, newStartValue);
  }

  @Test (expected = UnsupportedOperationException.class)
  public void read() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    URIStatus uriStatus = mFileSystem.getStatus(alluxioURI);
    try (FuseFileOutStream outStream = FuseFileOutStream.create(mFileSystem, mAuthPolicy,
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE, Optional.of(uriStatus))) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
      outStream.read(buffer, DEFAULT_FILE_LEN, 0);
    }
  }

  @Test (expected = UnsupportedOperationException.class)
  public void randomWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileOutStream outStream = FuseFileOutStream.create(mFileSystem, mAuthPolicy,
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE, Optional.empty())) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 15);
    }
  }

  @Test
  public void sequentialWriteAndgetFileLength() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileOutStream outStream = FuseFileOutStream.create(mFileSystem, mAuthPolicy,
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE, Optional.empty())) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileLength());
      buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileLength());
    }
    checkFileInAlluxio(alluxioURI, DEFAULT_FILE_LEN * 2, 0);
  }

  @Test
  public void truncateZeroOrDEFAULT_FILE_LENgth() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileOutStream outStream = FuseFileOutStream.create(mFileSystem, mAuthPolicy,
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE, Optional.empty())) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileLength());
      outStream.truncate(0);
      Assert.assertEquals(0, outStream.getFileLength());
      buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN * 2);
      outStream.write(buffer, DEFAULT_FILE_LEN * 2,  0);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN * 2);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, outStream.getFileLength());
    }
    checkFileInAlluxio(alluxioURI, DEFAULT_FILE_LEN * 2, 0);
  }

  @Test (expected = UnsupportedOperationException.class)
  public void truncateMiddle() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    try (FuseFileOutStream outStream = FuseFileOutStream.create(mFileSystem, mAuthPolicy,
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE, Optional.empty())) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      Assert.assertEquals(DEFAULT_FILE_LEN, outStream.getFileLength());
      outStream.truncate(DEFAULT_FILE_LEN / 2);
    }
  }
}
