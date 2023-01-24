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

package alluxio.client.fuse.dora.readonly.stream;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.fuse.file.FuseFileStream;
import alluxio.util.io.BufferUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * This class includes the tests for {@link alluxio.fuse.file.FuseFileInStream}.
 */
public class InStreamTest extends AbstractStreamTest {
  @Test
  public void createRead() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    URIStatus uriStatus = mFileSystem.getStatus(alluxioURI);
    try (FuseFileStream inStream = createStream(alluxioURI)) {
      Assert.assertEquals(uriStatus.getLength(), inStream.getFileStatus().getFileLength());
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN, inStream.read(buffer, DEFAULT_FILE_LEN, 0));
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, DEFAULT_FILE_LEN, buffer));
      Assert.assertEquals(uriStatus.getLength(), inStream.getFileStatus().getFileLength());
    }
  }

  @Test (expected = NotFoundRuntimeException.class)
  public void createNonexisting() {
    AlluxioURI alluxioURI = getTestFileUri();
    try (FuseFileStream inStream = createStream(alluxioURI)) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN, inStream.read(buffer, DEFAULT_FILE_LEN, 0));
    }
  }

  @Test
  public void randomRead() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream inStream = createStream(alluxioURI)) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN / 2);
      Assert.assertEquals(DEFAULT_FILE_LEN / 2,
          inStream.read(buffer, DEFAULT_FILE_LEN / 2, DEFAULT_FILE_LEN / 2));
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(
          DEFAULT_FILE_LEN / 2, DEFAULT_FILE_LEN / 2, buffer));
      buffer.clear();
      Assert.assertEquals(DEFAULT_FILE_LEN / 2,
          inStream.read(buffer, DEFAULT_FILE_LEN / 2, DEFAULT_FILE_LEN / 3));
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(
          DEFAULT_FILE_LEN / 3, DEFAULT_FILE_LEN / 2, buffer));
    }
  }

  protected FuseFileStream createStream(AlluxioURI uri) {
    return mStreamFactory
        .create(uri, OpenFlags.O_RDONLY.intValue(), DEFAULT_MODE.toShort());
  }
}
