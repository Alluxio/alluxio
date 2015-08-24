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

package tachyon.client.next.block;

import java.io.Closeable;
import java.io.IOException;

import tachyon.client.next.ClientOptions;
import tachyon.master.MasterClient;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.FileBlockInfo;

/**
 * Tachyon Block Store client. This is an internal client for all block level operations in Tachyon.
 * An instance of this class can be obtained via {@link TachyonBS#get}. This class is thread safe.
 */
public class TachyonBS implements Closeable {

  public static TachyonBS sCachedClient = null;

  public static synchronized TachyonBS get() {
    if (sCachedClient != null) {
      return sCachedClient;
    }
    sCachedClient = new TachyonBS();
    return sCachedClient;
  }

  private final BSContext mContext;

  public TachyonBS() {
    mContext = BSContext.INSTANCE;
  }

  public void close() {

  }

  /**
   * Gets the block info of a block, if it exists.
   *
   * @param blockId the blockId to obtain information about
   * @return a FileBlockInfo containing the metadata of the block
   * @throws IOException if the block does not exist
   */
  public FileBlockInfo getInfo(long blockId) throws IOException {
    MasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.user_getClientBlockInfo(blockId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets a stream to read the data of a block. The stream can be backed by Tachyon storage or
   * the under storage system.
   *
   * @param blockId the block to read from
   * @param options the custom options when reading the block
   * @return a BlockInStream which can be used to read the data in a streaming fashion
   * @throws IOException if the block does not exist
   */
  public BlockInStream getInStream(long blockId, ClientOptions options) throws IOException {
    MasterClient masterClient = mContext.acquireMasterClient();
    try {
      // TODO: Fix this RPC
      BlockInfo blockInfo = masterClient.user_getClientBlockInfo(blockId);
      return new ClientBlockInStream(blockInfo, options);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets a stream to write data to a block. The stream can only be backed by Tachyon storage.
   *
   * @param blockId the block to write
   * @param options the custom options when writing the block
   * @return a BlockOutStream which can be used to write data to the block in a streaming fashion
   * @throws IOException if the block cannot be written
   */
  public BlockOutStream getOutStream(long blockId, ClientOptions options) throws IOException {
    // No specified location to read from
    if (null == options.getLocation()) {
      // Local client, attempt to do direct write to local storage
      if (mContext.hasLocalWorker()) {
        return new LocalBlockOutStream(blockId, options);
      }
      // Client is not local or the data is not available on the local worker, use remote stream
      return new RemoteBlockOutStream(blockId, options);
    }
    // TODO: Handle the case when a location is specified
    return null;
  }
}
