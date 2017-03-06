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

package alluxio.client;

import alluxio.client.file.FileSystemContext;
import alluxio.client.netty.NettyUnderFileSystemFileReader;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * The interface to read an under file system file through a worker's data server.
 */
public interface UnderFileSystemFileReader extends Closeable {

  /**
   * The factory for the {@link UnderFileSystemFileReader}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory for {@link UnderFileSystemFileReader}.
     *
     * @param context the file system context
     * @return a new instance of {@link UnderFileSystemFileReader}
     */
    public static UnderFileSystemFileReader create(FileSystemContext context) {
      return new NettyUnderFileSystemFileReader(context);
    }
  }

  /**
   * Reads data from the specified worker for a file in the under file system.
   *
   * @param address the worker address to read from
   * @param ufsFileId the worker specific file id referencing the file to read
   * @param offset the offset in bytes in the file to read from
   * @param length the length in bytes to read
   * @return a byte buffer with the requested data, null if EOF is reached
   * @throws IOException if an error occurs communicating with the worker
   */
  ByteBuffer read(InetSocketAddress address, long ufsFileId, long offset, long length)
      throws IOException;
}

