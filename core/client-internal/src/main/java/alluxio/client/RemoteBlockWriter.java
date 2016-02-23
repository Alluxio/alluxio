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
     * @param conf Alluxio configuration
     * @return a new instance of {@link RemoteBlockWriter}
     */
    public static RemoteBlockWriter create(Configuration conf) {
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
