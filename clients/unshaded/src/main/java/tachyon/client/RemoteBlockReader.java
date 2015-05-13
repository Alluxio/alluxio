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

package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * The interface to read remote block from data server.
 */
public interface RemoteBlockReader {

  class Factory {
    public static RemoteBlockReader createRemoteBlockReader(TachyonConf conf) {
      try {
        return CommonUtils.createNewClassInstance(conf.getClass(Constants.USER_REMOTE_BLOCK_READER,
            ClientConstants.USER_REMOTE_BLOCK_READER_CLASS), null, null);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Read a remote block with a offset and length.
   *
   * @param host the remote data server hostname.
   * @param port the remote data server port number.
   * @param blockId the id of the block trying to read.
   * @param offset the offset of the block.
   * @param length the length the client wants to read.
   * @return
   * @throws IOException
   */
  ByteBuffer readRemoteBlock(String host, int port, long blockId, long offset,
      long length) throws IOException;
}
