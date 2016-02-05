/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import com.google.common.base.Throwables;

import alluxio.Constants;
import alluxio.Configuration;
import alluxio.util.CommonUtils;

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
