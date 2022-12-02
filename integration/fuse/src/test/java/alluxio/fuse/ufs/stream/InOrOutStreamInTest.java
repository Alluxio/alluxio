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
import alluxio.exception.runtime.AlreadyExistsRuntimeException;
import alluxio.fuse.file.FuseFileStream;

import jnr.constants.platform.OpenFlags;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * This class includes the read tests for {@link alluxio.fuse.file.FuseFileInOrOutStream}.
 */
public class InOrOutStreamInTest extends InStreamTest {
  @Override
  protected FuseFileStream createStream(AlluxioURI uri) {
    return mStreamFactory
        .create(uri, OpenFlags.O_RDWR.intValue(), DEFAULT_MODE.toShort());
  }

  @Override
  @Test(expected = AlreadyExistsRuntimeException.class)
  public void write() throws Exception {
    AlluxioURI alluxioURI = getTestFileUri();
    writeIncreasingByteArrayToFile(alluxioURI, DEFAULT_FILE_LEN);
    try (FuseFileStream inStream = createStream(alluxioURI)) {
      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put((byte) 'a');
      inStream.write(buffer, 1, 0);
    }
  }
}
