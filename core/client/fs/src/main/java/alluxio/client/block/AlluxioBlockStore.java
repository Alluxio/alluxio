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

import alluxio.Constants;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
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
import alluxio.network.TieredIdentityFactory;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio Block Store client. This is an internal client for all block level operations in Alluxio.
 * An instance of this class can be obtained via {@link AlluxioBlockStore} constructors.
 */
@ThreadSafe
public final class AlluxioBlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioBlockStore.class);

  private final FileSystemContext mContext;
  private final TieredIdentity mTieredIdentity;

  /**
   * Creates an Alluxio block store with default file system context and default local host name.
   *
   * @return the {@link AlluxioBlockStore} created
   */
  public static AlluxioBlockStore create() {
    return create(FileSystemContext.INSTANCE);
  }

  /**
   * Creates an Alluxio block store with default local hostname.
   *
   * @param context the file system context
   * @return the {@link AlluxioBlockStore} created
   */
  public static AlluxioBlockStore create(FileSystemContext context) {
    return new AlluxioBlockStore(context, TieredIdentityFactory.localIdentity());
  }

  /**
   * Creates an Alluxio block store.
   *
   * @param context the file system context
   * @param tieredIdentity the tiered identity
   */
  @VisibleForTesting
  AlluxioBlockStore(FileSystemContext context, TieredIdentity tieredIdentity) {
    mContext = context;
    mTieredIdentity = tieredIdentity;
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
   * @return the info of all block workers eligible for reads and writes
   */
  public List<BlockWorkerInfo> getEligibleWorkers() throws IOException {
    return getAllWorkers();
  }

  /**
   * @return the info of all block workers
   */
  public List<BlockWorkerInfo> getAllWorkers() throws IOException {
    try (CloseableResource<BlockMasterClient> masterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      return masterClientResource.get().getWorkerInfoList().stream()
          .map(w -> new BlockWorkerInfo(w.getAddress(), w.getCapacityBytes(), w.getUsedBytes()))
          .collect(Collectors.toList());
    }
  }

  /**
   * Gets a stream to read the data of a block. One of several possibilities, in order of priority:
   *
   * If there is a local worker:
   * 1. Domain socket - if the data is on the local worker & the local worker has a domain
   * socket server
   * 2. Short-Circuit - if the data is on the local worker
   * 3. Remote Read through worker - if the data is on a remote worker (worker -> worker -> client)
   * 4. UFS Read through worker - if the data is not in Alluxio
   *
   * If there is no local worker:
   * 1. Remote Read from worker - if the data is on a remote worker (worker -> client)
   * 2. UFS Read through worker - if the data is not in Alluxio
   *
   * @param blockId
   * @param options
   * @return
   */
  public BlockInStream getInStreamv2(long blockId, Protocol.OpenUfsBlockOptions ufsOptions,
      InStreamOptions options) throws IOException {
    // Determine the available locations of the block
    BlockInfo blockInfo;
    try (CloseableResource<BlockMasterClient> res = mContext.acquireBlockMasterClientResource()) {
      blockInfo = res.get().getBlockInfo(blockId);
    }
    List<BlockLocation> locations = blockInfo.getLocations();
    if (locations.isEmpty() && ufsOptions == null) {
      throw new NotFoundException(ExceptionMessage.BLOCK_UNAVAILABLE.getMessage(blockId));
    }
    // Determine which worker will serve the read request and the type of data source
    BlockInStreamSource dataSourceType = null;
    WorkerNetAddress dataSource = null;
    if (locations.isEmpty()) { // Data will be read from UFS
      BlockLocationPolicy policy = Preconditions.checkNotNull(options.getUfsReadLocationPolicy(),
          PreconditionMessage.UFS_READ_LOCATION_POLICY_UNSPECIFIED);
      GetWorkerOptions getWorkerOptions = GetWorkerOptions.defaults().setBlockId(blockId)
          .setBlockSize(blockInfo.getLength()).setBlockWorkerInfos(getEligibleWorkers());
      dataSourceType = BlockInStreamSource.UFS;
      dataSource = policy.getWorker(getWorkerOptions);
    } else { // Data will be read from Alluxio, determine which worker and if it is local
      List<TieredIdentity> tieredLocations =
          locations.stream().map(location -> location.getWorkerAddress().getTieredIdentity())
              .collect(Collectors.toList());
      Collections.shuffle(tieredLocations);
      Optional<TieredIdentity> nearest = mTieredIdentity.nearest(tieredLocations);
      if (nearest.isPresent()) {
        dataSource = blockInfo.getLocations().stream()
            .map(BlockLocation::getWorkerAddress)
            .filter(a -> a.getTieredIdentity().equals(nearest.get()))
            .findFirst().get();
        if (mTieredIdentity.getTier(0).getTierName().equals(Constants.LOCALITY_NODE)
            && mTieredIdentity.topTiersMatch(nearest.get())) {
          dataSourceType = BlockInStreamSource.LOCAL;
        } else {
          dataSourceType = BlockInStreamSource.REMOTE;
        }
      }
    }
    if (dataSource == null) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }

    if (mContext.hasLocalWorker()) {

    }

    // No local worker


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

    BlockInStreamSource source = BlockInStreamSource.UFS;
    if (blockInfo.getLocations().isEmpty() && openUfsBlockOptions == null) {
      throw new NotFoundException("Block " + blockId + " is unavailable in both Alluxio and UFS.");
    }
    WorkerNetAddress address = null;
    if (blockInfo.getLocations().isEmpty()) {
      BlockLocationPolicy blockLocationPolicy =
          Preconditions.checkNotNull(options.getUfsReadLocationPolicy(),
              PreconditionMessage.UFS_READ_LOCATION_POLICY_UNSPECIFIED);
      address = blockLocationPolicy
          .getWorker(GetWorkerOptions.defaults().setBlockWorkerInfos(getEligibleWorkers())
              .setBlockId(blockId).setBlockSize(blockInfo.getLength()));
    } else {
      // TODO(calvin): Get location via a policy.
      List<TieredIdentity> locations = blockInfo.getLocations().stream()
          .map(location -> location.getWorkerAddress().getTieredIdentity())
          .collect(Collectors.toList());
      Collections.shuffle(locations);
      Optional<TieredIdentity> nearest = mTieredIdentity.nearest(locations);
      if (nearest.isPresent()) {
        address = blockInfo.getLocations().stream()
            .map(BlockLocation::getWorkerAddress)
            .filter(a -> a.getTieredIdentity().equals(nearest.get()))
            .findFirst().get();
        if (mTieredIdentity.getTier(0).getTierName().equals(Constants.LOCALITY_NODE)
            && mTieredIdentity.topTiersMatch(nearest.get())) {
          source = BlockInStreamSource.LOCAL;
        } else {
          source = BlockInStreamSource.REMOTE;
        }
      }
    }
    if (address == null) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }

    LOG.debug(
        "Create block instream for {} of length {}  at address {},"
            + " using source: {}, openUfsBlockOptions: {}, options: {}",
        blockId, blockInfo.getLength(), address, source, openUfsBlockOptions, options);
    return BlockInStream.create(mContext, blockId, blockInfo.getLength(), address, source,
        openUfsBlockOptions, options);
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
   * @return an {@link BlockOutStream} which can be used to write data to the block in a streaming
   *         fashion
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
      throw new ResourceExhaustedException(ExceptionMessage.NO_SPACE_FOR_BLOCK_ON_WORKER
          .getMessage(FormatUtils.getSizeFromBytes(blockSize)));
    }
    LOG.debug("Create block outstream for {} of block size {} at address {}, using options: {}",
        blockId, blockSize, address, options);
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
   * @return a {@link BlockOutStream} which can be used to write data to the block in a streaming
   *         fashion
   */
  public BlockOutStream getOutStream(long blockId, long blockSize, OutStreamOptions options)
      throws IOException {
    WorkerNetAddress address;
    FileWriteLocationPolicy locationPolicy = Preconditions.checkNotNull(options.getLocationPolicy(),
        PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED);
    address = locationPolicy.getWorkerForNextBlock(getEligibleWorkers(), blockSize);
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
}
