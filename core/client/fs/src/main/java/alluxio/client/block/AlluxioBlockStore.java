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

package alluxio.client.block;

import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.exception.status.UnavailableException;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.util.FormatUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio Block Store client. This is an internal client for all block level operations in Alluxio.
 * An instance of this class can be obtained via {@link AlluxioBlockStore} constructors.
 */
@ThreadSafe
public final class AlluxioBlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioBlockStore.class);

  private final FileSystemContext mContext;
  private String mLocalHostName;
  private Random mRandom;

  /**
   * Creates an Alluxio block store with default file system context and default local host name.
   *
   * @return the {@link AlluxioBlockStore} created
   */
  public static AlluxioBlockStore create() {
    return new AlluxioBlockStore(FileSystemContext.INSTANCE,
        NetworkAddressUtils.getClientHostName());
  }

  /**
   * Creates an Alluxio block store with default local hostname.
   *
   * @param context the file system context
   * @return the {@link AlluxioBlockStore} created
   */
  public static AlluxioBlockStore create(FileSystemContext context) {
    return new AlluxioBlockStore(context, NetworkAddressUtils.getClientHostName());
  }

  /**
   * Creates an Alluxio block store.
   *
   * @param context the file system context
   * @param localHostName the local hostname for the block store
   */
  public AlluxioBlockStore(FileSystemContext context, String localHostName) {
    mContext = context;
    mLocalHostName = localHostName;
    mRandom = new Random();
  }

  /**
   * Gets the block info of a block, if it exists.
   *
   * @param blockId the blockId to obtain information about
   * @return a {@link BlockInfo} containing the metadata of the block
   */
  public BlockInfo getInfo(long blockId) throws IOException {
    try (CloseableResource<BlockMasterClient> masterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      return masterClientResource.get().getBlockInfo(blockId);
    }
  }

  /**
   * @return the info of all active block workers
   */
  public List<BlockWorkerInfo> getWorkerInfoList() throws IOException {
    List<BlockWorkerInfo> infoList = new ArrayList<>();
    try (CloseableResource<BlockMasterClient> masterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      for (WorkerInfo workerInfo : masterClientResource.get().getWorkerInfoList()) {
        infoList.add(new BlockWorkerInfo(workerInfo.getAddress(), workerInfo.getCapacityBytes(),
            workerInfo.getUsedBytes()));
      }
      return infoList;
    }
  }

  /**
   * Gets a stream to read the data of a block. The stream is backed by Alluxio storage.
   *
   * @param blockId the block to read from
   * @param openUfsBlockOptions the options to open UFS block, set to null if the block is not in
   *        UFS
   * @param options the options
   * @return an {@link InputStream} which can be used to read the data in a streaming fashion
   */
  public BlockInStream getInStream(long blockId, Protocol.OpenUfsBlockOptions openUfsBlockOptions,
      InStreamOptions options) throws IOException {
    BlockInfo blockInfo;
    try (CloseableResource<BlockMasterClient> masterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      blockInfo = masterClientResource.get().getBlockInfo(blockId);
    }

    if (blockInfo.getLocations().isEmpty() && openUfsBlockOptions == null) {
      throw new NotFoundException("Block " + blockId + " is unavailable in both Alluxio and UFS.");
    }
    WorkerNetAddress address = null;
    if (blockInfo.getLocations().isEmpty()) {
      BlockLocationPolicy blockLocationPolicy = Preconditions
          .checkNotNull(options.getUfsReadLocationPolicy(),
              PreconditionMessage.UFS_READ_LOCATION_POLICY_UNSPECIFIED);
      address = blockLocationPolicy.getWorker(
          GetWorkerOptions.defaults().setBlockWorkerInfos(getWorkerInfoList()).setBlockId(blockId)
              .setBlockSize(blockInfo.getLength()));
    }

    // TODO(calvin): Get location via a policy.
    // Although blockInfo.locations are sorted by tier, we prefer reading from the local worker.
    // But when there is no local worker or there are no local blocks, we prefer the first
    // location in blockInfo.locations that is nearest to memory tier.
    // Assuming if there is no local worker, there are no local blocks in blockInfo.locations.
    // TODO(cc): Check mContext.hasLocalWorker before finding for a local block when the TODO
    // for hasLocalWorker is fixed.
    for (BlockLocation location : blockInfo.getLocations()) {
      WorkerNetAddress workerNetAddress = location.getWorkerAddress();
      if (workerNetAddress.getHost().equals(mLocalHostName)) {
        address = workerNetAddress;
        break;
      }
    }
    if (address == null) {
      // No local worker/block, choose a random location. In the future we could change this to
      // only randomize among locations in the highest tier, or have the master randomize the order.
      List<BlockLocation> locations = blockInfo.getLocations();
      if (locations.isEmpty()) {
        throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
      }
      address = locations.get(mRandom.nextInt(locations.size())).getWorkerAddress();
    }
    return BlockInStream
        .create(mContext, blockId, blockInfo.getLength(), address, openUfsBlockOptions, options);
  }

  /**
   * Gets a stream to write data to a block. The stream can only be backed by Alluxio storage.
   *
   * @param blockId the block to write
   * @param blockSize the standard block size to write, or -1 if the block already exists (and this
   *        stream is just storing the block in Alluxio again)
   * @param address the address of the worker to write the block to, fails if the worker cannot
   *        serve the request
   * @param options the output stream options
   * @return an {@link BlockOutStream} which can be used to write data to the block in a
   *         streaming fashion
   */
  public BlockOutStream getOutStream(long blockId, long blockSize, WorkerNetAddress address,
      OutStreamOptions options) throws IOException {
    if (blockSize == -1) {
      try (CloseableResource<BlockMasterClient> blockMasterClientResource =
          mContext.acquireBlockMasterClientResource()) {
        blockSize = blockMasterClientResource.get().getBlockInfo(blockId).getLength();
      }
    }
    // No specified location to write to.
    if (address == null) {
      throw new ResourceExhaustedException(ExceptionMessage.NO_SPACE_FOR_BLOCK_ON_WORKER.getMessage(
          FormatUtils.getSizeFromBytes(blockSize)));
    }
    return BlockOutStream.create(mContext, blockId, blockSize, address, options);
  }

  /**
   * Gets a stream to write data to a block based on the options. The stream can only be backed by
   * Alluxio storage.
   *
   * @param blockId the block to write
   * @param blockSize the standard block size to write, or -1 if the block already exists (and this
   *        stream is just storing the block in Alluxio again)
   * @param options the output stream option
   * @return a {@link BlockOutStream} which can be used to write data to the block in a
   *         streaming fashion
   */
  public BlockOutStream getOutStream(long blockId, long blockSize, OutStreamOptions options)
      throws IOException {
    WorkerNetAddress address;
    FileWriteLocationPolicy locationPolicy = Preconditions.checkNotNull(options.getLocationPolicy(),
        PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED);
    address = locationPolicy.getWorkerForNextBlock(getWorkerInfoList(), blockSize);
    if (address == null) {
      throw new UnavailableException(
          ExceptionMessage.NO_SPACE_FOR_BLOCK_ON_WORKER.getMessage(blockSize));
    }
    return getOutStream(blockId, blockSize, address, options);
  }

  /**
   * Gets the total capacity of Alluxio's BlockStore.
   *
   * @return the capacity in bytes
   */
  public long getCapacityBytes() throws IOException {
    try (CloseableResource<BlockMasterClient> blockMasterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      return blockMasterClientResource.get().getCapacityBytes();
    }
  }

  /**
   * Gets the used bytes of Alluxio's BlockStore.
   *
   * @return the used bytes of Alluxio's BlockStore
   */
  public long getUsedBytes() throws IOException {
    try (CloseableResource<BlockMasterClient> blockMasterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      return blockMasterClientResource.get().getUsedBytes();
    }
  }

  /**
   * Sets the local host name. This is only used in the test.
   *
   * @param localHostName the local host name
   */
  public void setLocalHostName(String localHostName) {
    mLocalHostName = localHostName;
  }
}
