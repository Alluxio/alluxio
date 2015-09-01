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

import tachyon.client.BlockMasterClient;
import tachyon.client.next.ClientOptions;
import tachyon.thrift.BlockInfo;

/**
 * Tachyon Block Store client. This is an internal client for all block level operations in Tachyon.
 * An instance of this class can be obtained via {@link TachyonBS#get}. The methods in this class
 * are completely opaque to user input (such as {@link ClientOptions}). This class is thread safe.
 */
public class TachyonBS implements Closeable {

  private static TachyonBS sCachedClient = null;

  public static synchronized TachyonBS get() {
    if (null == sCachedClient) {
      sCachedClient = new TachyonBS();
    }
    return sCachedClient;
  }

  private final BSContext mContext;

  public TachyonBS() {
    mContext = BSContext.INSTANCE;
  }

  @Override
  // TODO: Evaluate the necessity of this method
  public synchronized void close() {
    sCachedClient = null;
  }

  /**
   * Gets the block info of a block, if it exists.
   *
   * @param blockId the blockId to obtain information about
   * @return a FileBlockInfo containing the metadata of the block
   * @throws IOException if the block does not exist
   */
  public BlockInfo getInfo(long blockId) throws IOException {
    BlockMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getBlockInfo(blockId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets a stream to read the data of a block. The stream is backed by Tachyon storage.
   *
   * @param blockId the block to read from
   * @return a BlockInStream which can be used to read the data in a streaming fashion
   * @throws IOException if the block does not exist
   */
  public BlockInStream getInStream(long blockId) throws IOException {
    BlockMasterClient masterClient = mContext.acquireMasterClient();
    try {
      // TODO: Fix this RPC
      BlockInfo blockInfo = masterClient.getBlockInfo(blockId);
      // TODO: Get location via a policy
      if (blockInfo.locations.isEmpty()) {
        // TODO: Maybe this shouldn't be an exception
        throw new IOException("No block " + blockId + " is not available in Tachyon");
      }
      return BlockInStream.get(blockId, blockInfo.getLength(), blockInfo.locations.get(0)
          .getWorkerAddress());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets a stream to write data to a block. The stream can only be backed by Tachyon storage.
   *
   * @param blockId the block to write
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
