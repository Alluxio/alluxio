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

package alluxio.client.fuse.ufs.stream;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.file.FuseFileStream;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.util.io.BufferUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * This class includes the write tests for {@link alluxio.fuse.file.FuseFileInOrOutStream}.
 */
public class InOrOutStreamOutTest extends OutStreamTest {
  @Override
  protected FuseFileStream createStream(AlluxioURI uri, boolean truncate) {
    int flags = OpenFlags.O_RDWR.intValue();
    if (truncate) {
      flags |= OpenFlags.O_TRUNC.intValue();
    }
    return mStreamFactory
        .create(uri, flags, DEFAULT_MODE.toShort());
  }

  @Test(expected = UnimplementedRuntimeException.class)
  public void read() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    try (FuseFileStream outStream = createStream(alluxioURI, false)) {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      outStream.write(buffer, DEFAULT_FILE_LEN, 0);
      outStream.read(buffer, DEFAULT_FILE_LEN, 0);
    }
  }

  @Test (expected = NotFoundRuntimeException.class)
  public void createEmpty() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    createStream(alluxioURI, false).close();
    URIStatus status = mFileSystem.getStatus(alluxioURI);
  }
}
