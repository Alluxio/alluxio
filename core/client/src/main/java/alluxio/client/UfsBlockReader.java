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
import alluxio.client.netty.NettyUfsBlockReader;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * The interface to read an under file system file through a worker's data server.
 */
public interface UfsBlockReader extends Closeable {

  /**
   * The factory for the {@link UfsBlockReader}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory for {@link UfsBlockReader}.
     *
     * @param context the file system context
     * @return a new instance of {@link UfsBlockReader}
     */
    public static UfsBlockReader create(FileSystemContext context) {
      return new NettyUfsBlockReader(context);
    }
  }

  /**
   * Reads a UFS block with a offset and length.
   *
   * @param address the {@link InetSocketAddress} of the data server
   * @param blockId the id of the block trying to read
   * @param offset the offset of the block
   * @param length the length the client wants to read
   * @param sessionId the session id of the client
   * @param noCache do not cache the data read from UFS in the Alluxio worker if set
   * @return a byte buffer containing the remote data block
   * @throws IOException if the remote server is not reachable or responds with failures
   */
  ByteBuffer read(InetSocketAddress address, long blockId, long offset, long length,
      long sessionId, boolean noCache) throws IOException;
}

