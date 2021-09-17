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

import alluxio.client.WriteType;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.block.stream.DataWriter;
import alluxio.client.block.util.BlockLocationUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.collections.Pair;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.network.TieredIdentityFactory;
import alluxio.resource.CloseableResource;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

// TODO(jianjian): rename AlluxioBlockStore since it would get confused by BlockStore
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
   * Creates an Alluxio block store with default local hostname.
   *
   * @param context the file system context
   * @return the {@link AlluxioBlockStore} created
   */
  public static AlluxioBlockStore create(FileSystemContext context) {
    return new AlluxioBlockStore(context,
        TieredIdentityFactory.localIdentity(context.getClusterConf()));
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
   * their most recently failed time and tries to update it when BlockInStream created failed,
   * attempting to avoid reading from a recently failed worker.
   *
   * @param blockId the id of the block to read
   * @param options the options associated with the read request
   * @param failedWorkers the map of workers address to most recent failure time
   * @return a stream which reads from the beginning of the block
   */
  public BlockInStream getInStream(long blockId, InStreamOptions options,
      Map<WorkerNetAddress, Long> failedWorkers) throws IOException {
    // Get the latest block info from master
    BlockInfo info = getInfo(blockId);
    return getInStream(info, options, failedWorkers);
  }

  /**
   * {@link #getInStream(long, InStreamOptions, Map)}.
   *
   * @param info the block info
   * @param options the options associated with the read request
   * @param failedWorkers the map of workers address to most recent failure time
   * @return a stream which reads from the beginning of the block
   */
  public BlockInStream getInStream(BlockInfo info, InStreamOptions options,
      Map<WorkerNetAddress, Long> failedWorkers) throws IOException {
    Pair<WorkerNetAddress, BlockInStreamSource> dataSourceAndType = getDataSourceAndType(info,
        options.getStatus(), options.getUfsReadLocationPolicy(), failedWorkers);
    WorkerNetAddress dataSource = dataSourceAndType.getFirst();
    BlockInStreamSource dataSourceType = dataSourceAndType.getSecond();
    try {
      return BlockInStream.create(mContext, info, dataSource, dataSourceType, options);
    } catch (UnavailableException e) {
      //When BlockInStream created failed, it will update the passed-in failedWorkers
      //to attempt to avoid reading from this failed worker in next try.
      failedWorkers.put(dataSource, System.currentTimeMillis());
      throw e;
    }
  }

  /**
   * Gets the data source and type of data source of a block. This method is primarily responsible
   * for determining the data source and type of data source. It takes a map of failed workers and
   * their most recently failed time and tries to update it when BlockInStream created failed,
   * attempting to avoid reading from a recently failed worker.
   *
   * @param info the info of the block to read
   * @param status the URIStatus associated with the read request
   * @param policy the policy determining the Alluxio worker location
   * @param failedWorkers the map of workers address to most recent failure time
   * @return the data source and type of data source of the block
   */
  public Pair<WorkerNetAddress, BlockInStreamSource> getDataSourceAndType(BlockInfo info,
      URIStatus status, BlockLocationPolicy policy, Map<WorkerNetAddress, Long> failedWorkers)
      throws IOException {
    List<BlockLocation> locations = info.getLocations();
    List<BlockWorkerInfo> blockWorkerInfo = Collections.EMPTY_LIST;
    // Initial target workers to read the block given the block locations.
    Set<WorkerNetAddress> workerPool;
    // Note that, it is possible that the blocks have been written as UFS blocks
    if (status.isPersisted()
        || status.getPersistenceState().equals("TO_BE_PERSISTED")) {
      blockWorkerInfo = mContext.getCachedWorkers();
      if (blockWorkerInfo.isEmpty()) {
        throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
      }
      workerPool = blockWorkerInfo.stream().map(BlockWorkerInfo::getNetAddress).collect(toSet());
    } else {
      if (locations.isEmpty()) {
        blockWorkerInfo = mContext.getCachedWorkers();
        if (blockWorkerInfo.isEmpty()) {
          throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
        }
        throw new UnavailableException(
            ExceptionMessage.BLOCK_UNAVAILABLE.getMessage(info.getBlockId()));
      }
      workerPool = locations.stream().map(BlockLocation::getWorkerAddress).collect(toSet());
    }
    // Workers to read the block, after considering failed workers.
    Set<WorkerNetAddress> workers = handleFailedWorkers(workerPool, failedWorkers);
    // TODO(calvin, jianjian): Consider containing these two variables in one object
    BlockInStreamSource dataSourceType = null;
    WorkerNetAddress dataSource = null;
    locations = locations.stream()
        .filter(location -> workers.contains(location.getWorkerAddress())).collect(toList());
    // First try to read data from Alluxio
    if (!locations.isEmpty()) {
      // TODO(calvin): Get location via a policy
      List<WorkerNetAddress> tieredLocations =
          locations.stream().map(location -> location.getWorkerAddress())
              .collect(toList());
      Collections.shuffle(tieredLocations);
      Optional<Pair<WorkerNetAddress, Boolean>> nearest =
          BlockLocationUtils.nearest(mTieredIdentity, tieredLocations, mContext.getClusterConf());
      if (nearest.isPresent()) {
        dataSource = nearest.get().getFirst();
        dataSourceType = nearest.get().getSecond() ? mContext.hasProcessLocalWorker()
            ? BlockInStreamSource.PROCESS_LOCAL : BlockInStreamSource.NODE_LOCAL
            : BlockInStreamSource.REMOTE;
      }
    }
    // Can't get data from Alluxio, get it from the UFS instead
    if (dataSource == null) {
      dataSourceType = BlockInStreamSource.UFS;
      Preconditions.checkNotNull(policy,
              PreconditionMessage.UFS_READ_LOCATION_POLICY_UNSPECIFIED);
      blockWorkerInfo = blockWorkerInfo.stream()
          .filter(workerInfo -> workers.contains(workerInfo.getNetAddress())).collect(toList());
      GetWorkerOptions getWorkerOptions = GetWorkerOptions.defaults()
          .setBlockInfo(new BlockInfo()
              .setBlockId(info.getBlockId())
              .setLength(info.getLength())
              .setLocations(locations))
          .setBlockWorkerInfos(blockWorkerInfo);
      dataSource = policy.getWorker(getWorkerOptions);
      if (dataSource != null) {
        if (mContext.hasProcessLocalWorker()
            && dataSource.equals(mContext.getNodeLocalWorker())) {
          dataSourceType = BlockInStreamSource.PROCESS_LOCAL;
          LOG.debug("Create BlockInStream to read data from UFS through process local worker {}",
              dataSource);
        } else {
          LOG.debug("Create BlockInStream to read data from UFS through worker {} "
              + "(client embedded in local worker process: {},"
                  + "client co-located with worker in different processes: {}, "
                  + "local worker address: {})",
              dataSource, mContext.hasProcessLocalWorker(), mContext.hasNodeLocalWorker(),
              mContext.hasNodeLocalWorker() ? mContext.getNodeLocalWorker() : "N/A");
        }
      }
    }

    if (dataSource == null) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    return new Pair<>(dataSource, dataSourceType);
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
   * @param blockSize the standard block size to write
   * @param address the address of the worker to write the block to, fails if the worker cannot
   *        serve the request
   * @param options the output stream options
   * @return an {@link BlockOutStream} which can be used to write data to the block in a streaming
   *         fashion
   */
  public BlockOutStream getOutStream(long blockId, long blockSize, WorkerNetAddress address,
      OutStreamOptions options) throws IOException {
    // No specified location to write to.
    Preconditions.checkNotNull(address, "address");
    LOG.debug("Create BlockOutStream for {} of block size {} at address {}, using options: {}",
        blockId, blockSize, address, options);
    DataWriter dataWriter =
        DataWriter.Factory.create(mContext, blockId, blockSize, address, options);
    return new BlockOutStream(dataWriter, blockSize, address);
  }

  /**
   * Gets a stream to write data to a block based on the options. The stream can only be backed by
   * Alluxio storage.
   *
   * @param blockId the block to write
   * @param blockSize the standard block size to write
   * @param options the output stream option
   * @return a {@link BlockOutStream} which can be used to write data to the block in a streaming
   *         fashion
   */
  public BlockOutStream getOutStream(long blockId, long blockSize, OutStreamOptions options)
      throws IOException {
    WorkerNetAddress address;
    BlockLocationPolicy locationPolicy = Preconditions.checkNotNull(options.getLocationPolicy(),
        PreconditionMessage.BLOCK_WRITE_LOCATION_POLICY_UNSPECIFIED);
    GetWorkerOptions workerOptions = GetWorkerOptions.defaults()
        .setBlockInfo(new BlockInfo().setBlockId(blockId).setLength(blockSize))
        .setBlockWorkerInfos(new ArrayList<>(mContext.getCachedWorkers()));

    // The number of initial copies depends on the write type: if ASYNC_THROUGH, it is the property
    // "alluxio.user.file.replication.durable" before data has been persisted; otherwise
    // "alluxio.user.file.replication.min"
    int initialReplicas = (options.getWriteType() == WriteType.ASYNC_THROUGH
        && options.getReplicationDurable() > options.getReplicationMin())
        ? options.getReplicationDurable() : options.getReplicationMin();
    if (initialReplicas <= 1) {
      address = locationPolicy.getWorker(workerOptions);
      if (address == null) {
        if (mContext.getCachedWorkers().isEmpty()) {
          throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
        }
        throw new UnavailableException(
            ExceptionMessage.NO_SPACE_FOR_BLOCK_ON_WORKER.getMessage(blockSize));
      }
      // TODO(ggezer): Retry on another worker if this has no storage.
      return getOutStream(blockId, blockSize, address, options);
    }

    // Group different block workers by their hostnames
    Map<String, Set<BlockWorkerInfo>> blockWorkersByHost = new HashMap<>();
    for (BlockWorkerInfo blockWorker : workerOptions.getBlockWorkerInfos()) {
      String hostName = blockWorker.getNetAddress().getHost();
      if (blockWorkersByHost.containsKey(hostName)) {
        blockWorkersByHost.get(hostName).add(blockWorker);
      } else {
        blockWorkersByHost.put(hostName, com.google.common.collect.Sets.newHashSet(blockWorker));
      }
    }

    // Select N workers on different hosts where N is the value of initialReplicas for this block
    List<WorkerNetAddress> workerAddressList = new ArrayList<>();
    List<BlockWorkerInfo> updatedInfos = Lists.newArrayList(workerOptions.getBlockWorkerInfos());
    for (int i = 0; i < initialReplicas; i++) {
      address = locationPolicy.getWorker(workerOptions);
      if (address == null) {
        break;
      }
      workerAddressList.add(address);
      updatedInfos.removeAll(blockWorkersByHost.get(address.getHost()));
      workerOptions.setBlockWorkerInfos(updatedInfos);
    }
    if (workerAddressList.size() < initialReplicas) {
      throw new alluxio.exception.status.ResourceExhaustedException(String.format(
          "Not enough workers for replications, %d workers selected but %d required",
          workerAddressList.size(), initialReplicas));
    }
    return BlockOutStream
        .createReplicatedBlockOutStream(mContext, blockId, blockSize, workerAddressList, options);
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
