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

import com.google.common.base.Throwables;

import alluxio.Constants;
import alluxio.conf.TachyonConf;
import alluxio.util.CommonUtils;

/**
 * The interface to write a remote block to the data server.
 */
public interface RemoteBlockWriter extends Closeable {

  /**
   * The factory for the {@link RemoteBlockWriter}.
   */
  class Factory {
    /**
     * Factory for {@link RemoteBlockWriter}.
     *
     * @param conf Tachyon configuration
     * @return a new instance of {@link RemoteBlockWriter}
     */
    public static RemoteBlockWriter create(TachyonConf conf) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.<RemoteBlockWriter>getClass(Constants.USER_BLOCK_REMOTE_WRITER), null, null);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Opens a block writer to a data server.
   *
   * @param address the {@link InetSocketAddress} of the data server
   * @param blockId the id of the block to write
   * @param sessionId the id of the session writing the block
   * @throws IOException when the operation fails
   */
  void open(InetSocketAddress address, long blockId, long sessionId) throws IOException;

  /**
   * Writes data to the remote block.
   *
   * @param bytes an array of bytes representing the source data
   * @param offset the offset into the source array of bytes
   * @param length the length of the data to write (in bytes)
   * @throws IOException when the operation fails
   */
  void write(byte[] bytes, int offset, int length) throws IOException;
}
