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

package tachyon.client.block;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.base.Preconditions;

import tachyon.client.BlockMasterClient;
import tachyon.client.ClientContext;
import tachyon.client.ClientOptions;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerClient;

/**
 * Tachyon Block Store client. This is an internal client for all block level operations in Tachyon.
 * An instance of this class can be obtained via {@link TachyonBlockStore#get}. The methods in this
 * class are completely opaque to user input (such as {@link ClientOptions}). This class is thread
 * safe.
 */
public class TachyonBlockStore implements Closeable {

  private static TachyonBlockStore sClient = null;

  /**
   * @return a new instance of Tachyon block store
   */
  public static synchronized TachyonBlockStore get() {
    if (sClient == null) {
      sClient = new TachyonBlockStore();
    }
    return sClient;
  }

  private final BlockStoreContext mContext;

  /**
   * Creates a Tachyon block store.
   */
  private TachyonBlockStore() {
    mContext = BlockStoreContext.INSTANCE;
  }

  @Override
  // TODO(calvin): Evaluate the necessity of this method.
  public synchronized void close() {
    sClient = null;
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
      // TODO(calvin): Fix this RPC.
      BlockInfo blockInfo = masterClient.getBlockInfo(blockId);
      // TODO(calvin): Get location via a policy.
      if (blockInfo.locations.isEmpty()) {
        // TODO(calvin): Maybe this shouldn't be an exception.
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
   * @param blockSize the standard block size to write, or -1 if the block already exists (and
   *                  this stream is just storing the block in Tachyon again)
   * @param location the worker to write the block to, fails if the worker cannot serve the request
   * @return a BlockOutStream which can be used to write data to the block in a streaming fashion
   * @throws IOException if the block cannot be written
   */
  public BufferedBlockOutStream getOutStream(long blockId, long blockSize, NetAddress location)
      throws IOException {
    if (blockSize == -1) {
      BlockMasterClient blockMasterClient = mContext.acquireMasterClient();
      try {
        blockSize = blockMasterClient.getBlockInfo(blockId).getLength();
      } finally {
        mContext.releaseMasterClient(blockMasterClient);
      }
    }
    // No specified location to write to.
    if (location == null) {
      // Local client, attempt to do direct write to local storage.
      if (mContext.hasLocalWorker()) {
        return new LocalBlockOutStream(blockId, blockSize);
      }
      // Client is not local or the data is not available on the local worker, use remote stream.
      return new RemoteBlockOutStream(blockId, blockSize);
    }
    // Location is local.
    if (NetworkAddressUtils.getLocalHostName(ClientContext.getConf()).equals(location.getHost())) {
      Preconditions.checkState(mContext.hasLocalWorker(), "Requested write location unavailable.");
      return new LocalBlockOutStream(blockId, blockSize);
    }
    // TODO(calvin): Handle the case when a location is specified.
    return null;
  }

  /**
   * Gets the total capacity of Tachyon's BlockStore.
   *
   * @return the capacity in bytes
   * @throws IOException
   */
  public long getCapacityBytes() throws IOException {
    BlockMasterClient blockMasterClient = mContext.acquireMasterClient();
    try {
      return blockMasterClient.getCapacityBytes();
    } finally {
      mContext.releaseMasterClient(blockMasterClient);
    }
  }

  /**
   * Gets the used bytes of Tachyon's BlockStore.
   *
   * @throws IOException
   */
  public long getUsedBytes() throws IOException {
    BlockMasterClient blockMasterClient = mContext.acquireMasterClient();
    try {
      return blockMasterClient.getUsedBytes();
    } finally {
      mContext.releaseMasterClient(blockMasterClient);
    }
  }

  /**
   * Attempts to promote a block in Tachyon space. If the block is not present, this method will
   * return without an error. If the block is present in multiple workers, only one worker will
   * receive the promotion request.
   *
   * @param blockId the id of the block to promote
   * @throws IOException if the block does not exist
   */
  public void promote(long blockId) throws IOException {
    BlockMasterClient blockMasterClient = mContext.acquireMasterClient();
    try {
      BlockInfo info = blockMasterClient.getBlockInfo(blockId);
      if (info.getLocations().isEmpty()) {
        // Nothing to promote
        return;
      }
      // Get the first worker address for now, as this will likely be the location being read from
      // TODO: Get this location via a policy (possibly location is a parameter to promote)
      NetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
      WorkerClient workerClient = mContext.acquireWorkerClient(workerAddr.getHost());
      try {
        workerClient.promoteBlock(blockId);
      } finally {
        mContext.releaseWorkerClient(workerClient);
      }
    } finally {
      mContext.releaseMasterClient(blockMasterClient);
    }
  }
}
