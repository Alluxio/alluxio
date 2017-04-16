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
import alluxio.client.netty.NettyUnderFileSystemFileWriter;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * The interface to write an under file system file through a worker's data server.
 */
public interface UnderFileSystemFileWriter extends Closeable {

  /**
   * The factory for the {@link UnderFileSystemFileWriter}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory for {@link UnderFileSystemFileWriter}.
     *
     * @return a new instance of {@link UnderFileSystemFileWriter}
     */
    public static UnderFileSystemFileWriter create(FileSystemContext context) {
      return new NettyUnderFileSystemFileWriter(context);
    }
  }

  /**
   * Writes data to the file in the under file system.
   *
   * @param address worker address to write the data to
   * @param ufsFileId worker file id referencing the file
   * @param fileOffset the offset in bytes where in the file to start writing, only sequential
   *                   writes are supported
   * @param source source data to write
   * @param offset start offset in bytes of the data in byte array source
   * @param length length in bytes to write
   * @throws IOException if an error occurs during the write
   */
  void write(InetSocketAddress address, long ufsFileId, long fileOffset, byte[] source,
      int offset, int length) throws IOException;
}

