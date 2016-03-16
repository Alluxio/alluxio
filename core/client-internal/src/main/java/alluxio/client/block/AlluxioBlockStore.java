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

package alluxio.client.block;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.ExceptionMessage;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio Block Store client. This is an internal client for all block level operations in Alluxio.
 * An instance of this class can be obtained via {@link AlluxioBlockStore#get()}. The methods in
 * this class are completely opaque to user input.
 */
@ThreadSafe
public final class AlluxioBlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static AlluxioBlockStore sClient = null;

  /**
   * @return a new instance of Alluxio block store
   */
  public static synchronized AlluxioBlockStore get() {
    if (sClient == null) {
      sClient = new AlluxioBlockStore();
    }
    return sClient;
  }

  private final BlockStoreContext mContext;

  /**
   * Creates an Alluxio block store.
   */
  private AlluxioBlockStore() {
    mContext = BlockStoreContext.INSTANCE;
  }

  /**
   * Gets the block info of a block, if it exists.
   *
   * @param blockId the blockId to obtain information about
   * @return a {@link BlockInfo} containing the metadata of the block
   * @throws IOException if the block does not exist
   */
  public BlockInfo getInfo(long blockId) throws IOException {
    BlockMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getBlockInfo(blockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * @return the info of all active block workers
   * @throws IOException when work info list cannot be obtained from master
   * @throws AlluxioException if network connection failed
   */
  public List<BlockWorkerInfo> getWorkerInfoList() throws IOException, AlluxioException {
    List<BlockWorkerInfo> infoList = Lists.newArrayList();
    BlockMasterClient masterClient = mContext.acquireMasterClient();
    try {
      for (WorkerInfo workerInfo : masterClient.getWorkerInfoList()) {
        infoList.add(new BlockWorkerInfo(workerInfo.getAddress(), workerInfo.getCapacityBytes(),
            workerInfo.getUsedBytes()));
      }
      return infoList;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets a stream to read the data of a block. The stream is backed by Alluxio storage.
   *
   * @param blockId the block to read from
   * @return a {@link BlockInStream} which can be used to read the data in a streaming fashion
   * @throws IOException if the block does not exist
   */
  public BufferedBlockInStream getInStream(long blockId) throws IOException {
    BlockMasterClient masterClient = mContext.acquireMasterClient();
    BlockInfo blockInfo;
    try {
      blockInfo = masterClient.getBlockInfo(blockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }

    if (blockInfo.getLocations().isEmpty()) {
      throw new IOException("Block " + blockId + " is not available in Alluxio");
    }
    // TODO(calvin): Get location via a policy.
    // Although blockInfo.locations are sorted by tier, we prefer reading from the local worker.
    // But when there is no local worker or there are no local blocks, we prefer the first
    // location in blockInfo.locations that is nearest to memory tier.
    // Assuming if there is no local worker, there are no local blocks in blockInfo.locations.
    // TODO(cc): Check mContext.hasLocalWorker before finding for a local block when the TODO
    // for hasLocalWorker is fixed.
    String localHostName = NetworkAddressUtils.getLocalHostName(ClientContext.getConf());
    for (BlockLocation location : blockInfo.getLocations()) {
      WorkerNetAddress workerNetAddress = location.getWorkerAddress();
      if (workerNetAddress.getHost().equals(localHostName)) {
        // There is a local worker and the block is local.
        try {
          return new LocalBlockInStream(blockId, blockInfo.getLength());
        } catch (IOException e) {
          LOG.warn("Failed to open local stream for block " + blockId + ". " + e.getMessage());
          // Getting a local stream failed, do not try again
          break;
        }
      }
    }
    // No local worker/block, get the first location since it's nearest to memory tier.
    WorkerNetAddress workerNetAddress = blockInfo.getLocations().get(0).getWorkerAddress();
    return new RemoteBlockInStream(blockId, blockInfo.getLength(), workerNetAddress);
  }

  /**
   * Gets a stream to write data to a block. The stream can only be backed by Alluxio storage.
   *
   * @param blockId the block to write
   * @param blockSize the standard block size to write, or -1 if the block already exists (and this
   *        stream is just storing the block in Alluxio again)
   * @param address the address of the worker to write the block to, fails if the worker cannot
   *        serve the request
   * @return a {@link BufferedBlockOutStream} which can be used to write data to the block in a
   *         streaming fashion
   * @throws IOException if the block cannot be written
   */
  public BufferedBlockOutStream getOutStream(long blockId, long blockSize, WorkerNetAddress address)
      throws IOException {
    if (blockSize == -1) {
      BlockMasterClient blockMasterClient = mContext.acquireMasterClient();
      try {
        blockSize = blockMasterClient.getBlockInfo(blockId).getLength();
      } catch (AlluxioException e) {
        throw new IOException(e);
      } finally {
        mContext.releaseMasterClient(blockMasterClient);
      }
    }
    // No specified location to write to.
    if (address == null) {
      throw new RuntimeException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    // Location is local.
    if (NetworkAddressUtils.getLocalHostName(ClientContext.getConf()).equals(address.getHost())) {
      if (mContext.hasLocalWorker()) {
        return new LocalBlockOutStream(blockId, blockSize);
      } else {
        throw new IOException(ExceptionMessage.NO_LOCAL_WORKER.getMessage("write"));
      }
    }
    // Location is specified and it is remote.
    return new RemoteBlockOutStream(blockId, blockSize, address);
  }

  /**
   * Gets the total capacity of Alluxio's BlockStore.
   *
   * @return the capacity in bytes
   * @throws IOException when the connection to the client fails
   */
  public long getCapacityBytes() throws IOException {
    BlockMasterClient blockMasterClient = mContext.acquireMasterClient();
    try {
      return blockMasterClient.getCapacityBytes();
    } catch (ConnectionFailedException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(blockMasterClient);
    }
  }

  /**
   * Gets the used bytes of Alluxio's BlockStore.
   *
   * @return the used bytes of Alluxio's BlockStore
   * @throws IOException when the connection to the client fails
   */
  public long getUsedBytes() throws IOException {
    BlockMasterClient blockMasterClient = mContext.acquireMasterClient();
    try {
      return blockMasterClient.getUsedBytes();
    } catch (ConnectionFailedException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(blockMasterClient);
    }
  }

  /**
   * Attempts to promote a block in Alluxio space. If the block is not present, this method will
   * return without an error. If the block is present in multiple workers, only one worker will
   * receive the promotion request.
   *
   * @param blockId the id of the block to promote
   * @throws IOException if the block does not exist
   */
  public void promote(long blockId) throws IOException {
    BlockMasterClient blockMasterClient = mContext.acquireMasterClient();
    BlockInfo info;
    try {
      info = blockMasterClient.getBlockInfo(blockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(blockMasterClient);
    }
    if (info.getLocations().isEmpty()) {
      // Nothing to promote
      return;
    }
    // Get the first worker address for now, as this will likely be the location being read from
    // TODO(calvin): Get this location via a policy (possibly location is a parameter to promote)
    WorkerNetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
    BlockWorkerClient blockWorkerClient = mContext.acquireWorkerClient(workerAddr.getHost());
    try {
      blockWorkerClient.promoteBlock(blockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseWorkerClient(blockWorkerClient);
    }
  }
}
