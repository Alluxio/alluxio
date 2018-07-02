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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import alluxio.Constants;
import alluxio.MetaCache;
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
import alluxio.resource.CloseableResource;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

  private static List<String> readerExHosts = null;
  {
      String hosts = System.getenv("QINIU_READER_EX_HOSTS");
      if (hosts != null) readerExHosts = Arrays.asList(hosts.split("\\s*,\\s*"));
      if (readerExHosts == null) readerExHosts = new ArrayList<String>();
  }

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
    BlockInfo info = MetaCache.getBlockInfoCache(blockId); //qiniu2
    if (info != null) return info;
    try (CloseableResource<BlockMasterClient> masterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      //return masterClientResource.get().getBlockInfo(blockId); -- qiniu2
      info = masterClientResource.get().getBlockInfo(blockId);
      if (info != null) MetaCache.addBlockInfoCache(blockId, info); //qiniu2
      return info;
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
          .collect(toList());
    }
  }

  /**
   * Gets a stream to read the data of a block. This method is primarily responsible for
   * determining the data source and type of data source. The latest BlockInfo will be fetched
   * from the master to ensure the locations are up to date.
   *
   * @param blockId the id of the block to read
   * @param options the options associated with the read request
   * @return a stream which reads from the beginning of the block
   */
  public BlockInStream getInStream(long blockId, InStreamOptions options) throws IOException {
    return getInStream(blockId, options, ImmutableMap.of());
  }

  /**
   * Gets a stream to read the data of a block. This method is primarily responsible for
   * determining the data source and type of data source. The latest BlockInfo will be fetched
   * from the master to ensure the locations are up to date. It takes a map of failed workers and
   * their most recently failed time and attempts to avoid reading from a recently failed worker.
   *
   * @param blockId the id of the block to read
   * @param options the options associated with the read request
   * @param failedWorkers the map of workers address to most recent failure time
   * @return a stream which reads from the beginning of the block
   */
  public BlockInStream getInStream(long blockId, InStreamOptions options,
      Map<WorkerNetAddress, Long> failedWorkers) throws IOException {
    /*try (CloseableResource<BlockMasterClient> masterClientResource =
             mContext.acquireBlockMasterClientResource()) {
      info = masterClientResource.get().getBlockInfo(blockId);
      
    }*/
    BlockInfo info = getInfo(blockId);
    List<BlockLocation> locations = info.getLocations();

    //qiniu
    Set<WorkerNetAddress> cachedWorkers = MetaCache.getWorkerInfoList().stream().map(WorkerInfo::getAddress).collect(toSet());
    Set<WorkerNetAddress> blockWorkers = locations.stream().map(BlockLocation::getWorkerAddress).collect(toSet());
    if (!cachedWorkers.containsAll(blockWorkers)) MetaCache.invalidateWorkerInfoList();

    List<BlockWorkerInfo> blockWorkerInfo = Collections.EMPTY_LIST;
    // Initial target workers to read the block given the block locations.
    // By default, read will no go to QINIU_READER_EX_HOSTS unless the block is already there - qiniu
    Set<WorkerNetAddress> workerPool;
    if (options.getStatus().isPersisted()) {
      blockWorkerInfo = getEligibleWorkers();
      workerPool = blockWorkerInfo.stream().map(BlockWorkerInfo::getNetAddress).collect(toSet());
      if (readerExHosts.size() > 0) {
          Set<WorkerNetAddress> subPool = workerPool.stream()
              .filter(w -> blockWorkers.contains(w) || !readerExHosts.contains(w.getHost() + ":" + w.getDataPort()))
              .collect(toSet());
          if (!failedWorkers.keySet().containsAll(subPool)) workerPool = subPool;
      }
    } else {
      workerPool = locations.stream().map(BlockLocation::getWorkerAddress).collect(toSet());
    }
    if (workerPool.isEmpty()) {
      MetaCache.invalidateBlockInfoCache(blockId); //qiniu
      throw new NotFoundException(ExceptionMessage.BLOCK_UNAVAILABLE.getMessage(info.getBlockId()));
    }
    // Workers to read the block, after considering failed workers.
    Set<WorkerNetAddress> workers = handleFailedWorkers(workerPool, failedWorkers);
    // TODO(calvin): Consider containing these two variables in one object
    BlockInStreamSource dataSourceType = null;
    WorkerNetAddress dataSource = null;
    locations = locations.stream()
        .filter(location -> workers.contains(location.getWorkerAddress())).collect(toList());
    // First try to read data from Alluxio
    if (!locations.isEmpty()) {
      // TODO(calvin): Get location via a policy
      List<TieredIdentity> tieredLocations =
          locations.stream().map(location -> location.getWorkerAddress().getTieredIdentity())
              .collect(toList());
      Collections.shuffle(tieredLocations);
      Optional<TieredIdentity> nearest = mTieredIdentity.nearest(tieredLocations);
      if (nearest.isPresent()) {
        dataSource = locations.stream().map(BlockLocation::getWorkerAddress)
            .filter(addr -> addr.getTieredIdentity().equals(nearest.get())).findFirst().get();
        if (mTieredIdentity.getTier(0).getTierName().equals(Constants.LOCALITY_NODE)
            && mTieredIdentity.topTiersMatch(nearest.get())) {
          dataSourceType = BlockInStreamSource.LOCAL;
        } else {
          dataSourceType = BlockInStreamSource.REMOTE;
        }
      }
    } else {
        MetaCache.invalidateBlockInfoCache(blockId);
    }
    // Can't get data from Alluxio, get it from the UFS instead
    if (dataSource == null) {
      MetaCache.invalidateBlockInfoCache(blockId);
      dataSourceType = BlockInStreamSource.UFS;
      BlockLocationPolicy policy =
          Preconditions.checkNotNull(options.getOptions().getUfsReadLocationPolicy(),
              PreconditionMessage.UFS_READ_LOCATION_POLICY_UNSPECIFIED);
      blockWorkerInfo = blockWorkerInfo.stream()
          .filter(workerInfo -> workers.contains(workerInfo.getNetAddress())).collect(toList());
      GetWorkerOptions getWorkerOptions = GetWorkerOptions.defaults().setBlockId(info.getBlockId())
          .setBlockSize(info.getLength()).setBlockWorkerInfos(blockWorkerInfo);
      dataSource = policy.getWorker(getWorkerOptions);
    }
    if (dataSource == null) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    return BlockInStream.create(mContext, info, dataSource, dataSourceType, options);
  }

  private Set<WorkerNetAddress> handleFailedWorkers(Set<WorkerNetAddress> workers,
      Map<WorkerNetAddress, Long> failedWorkers) {
    if (workers.isEmpty()) {
      return Collections.EMPTY_SET;
    }
    Set<WorkerNetAddress> nonFailed =
        workers.stream().filter(worker -> !failedWorkers.containsKey(worker)).collect(toSet());
    if (nonFailed.isEmpty()) {
      return Collections.singleton(workers.stream()
          .min((x, y) -> Long.compare(failedWorkers.get(x), failedWorkers.get(y))).get());
    }
    return nonFailed;
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
      /*
      try (CloseableResource<BlockMasterClient> blockMasterClientResource =
          mContext.acquireBlockMasterClientResource()) {
        blockSize = blockMasterClientResource.get().getBlockInfo(blockId).getLength();
      }*/
      blockSize = getInfo(blockId).getLength();
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
