/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * The interface to read remote block from data server.
 */
public interface RemoteBlockReader extends Closeable {

  /**
   * The factory for the {@link RemoteBlockReader}.
   */
  class Factory {
    /**
     * Factory for {@link RemoteBlockReader}.
     *
     * @param conf Alluxio configuration
     * @return a new instance of {@link RemoteBlockReader}
     */
    public static RemoteBlockReader create(Configuration conf) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.<RemoteBlockReader>getClass(Constants.USER_BLOCK_REMOTE_READER), null, null);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Reads a remote block with a offset and length.
   *
   * @param address the {@link InetSocketAddress} of the data server
   * @param blockId the id of the block trying to read
   * @param offset the offset of the block
   * @param length the length the client wants to read
   * @param lockId the acquired block lock id
   * @param sessionId the session id of the client
   * @return a byte buffer containing the remote data block
   * @throws IOException if the remote server is not reachable or responds with failures
   */
  ByteBuffer readRemoteBlock(InetSocketAddress address, long blockId, long offset,
      long length, long lockId, long sessionId) throws IOException;
}
