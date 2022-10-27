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
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.file.FuseFileInStream;
import alluxio.fuse.file.FuseFileStream;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Integration test for {@link alluxio.fuse.file.FuseFileInStream}.
 */
public class FuseFileInStreamIntegrationTest extends AbstractFuseFileStreamIntegrationTest {
  @Test
  public void createRead() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    URIStatus uriStatus = mFileSystem.getStatus(alluxioURI);
    try (FuseFileStream inStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_RDONLY.intValue(), MODE)) {
      Assert.assertEquals(uriStatus.getLength(), inStream.getFileLength());
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN, inStream.read(buffer, DEFAULT_FILE_LEN, 0));
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, DEFAULT_FILE_LEN, buffer));
      Assert.assertEquals(uriStatus.getLength(), inStream.getFileLength());
    }
  }

  @Test (expected = NotFoundRuntimeException.class)
  public void createNonexisting() {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    FuseFileInStream.create(mFileSystem, alluxioURI, Optional.empty());
  }

  @Test
  public void randomRead() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream inStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_RDONLY.intValue(), MODE)) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN / 2);
      Assert.assertEquals(DEFAULT_FILE_LEN / 2,
          inStream.read(buffer, DEFAULT_FILE_LEN / 2, DEFAULT_FILE_LEN / 3));
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(
          DEFAULT_FILE_LEN / 3, DEFAULT_FILE_LEN / 2, buffer));
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void write() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream inStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_RDONLY.intValue(), MODE)) {
      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put((byte) 'a');
      inStream.write(buffer, 1, 0);
    }
  }

  @Test (expected = UnimplementedRuntimeException.class)
  public void truncate() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream inStream = mStreamFactory
        .create(alluxioURI, OpenFlags.O_RDONLY.intValue(), MODE)) {
      inStream.truncate(0);
    }
  }
}
