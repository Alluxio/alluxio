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

package alluxio.job.replicate;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.SerializableVoid;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job to evict a block. This job is invoked by the checker of replication level in
 * FileSystemMaster.
 */
@NotThreadSafe
public final class EvictDefinition
    extends AbstractVoidJobDefinition<EvictConfig, SerializableVoid> {
  private static final Logger LOG = LoggerFactory.getLogger(EvictDefinition.class);

  private final FileSystemContext mFileSystemContext;

  /**
   * Constructs a new {@link EvictDefinition}.
   */
  public EvictDefinition() {
    mFileSystemContext = FileSystemContext.get();
  }

  /**
   * Constructs a new {@link EvictDefinition} with FileSystem context and instance.
   *
   * @param fileSystemContext file system context
   */
  public EvictDefinition(FileSystemContext fileSystemContext) {
    mFileSystemContext = fileSystemContext;
  }

  @Override
  public Class<EvictConfig> getJobConfigClass() {
    return EvictConfig.class;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(EvictConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Preconditions.checkArgument(!jobWorkerInfoList.isEmpty(), "No worker is available");

    long blockId = config.getBlockId();
    int numReplicas = config.getReplicas();

    AlluxioBlockStore blockStore = AlluxioBlockStore.create(mFileSystemContext);
    BlockInfo blockInfo = blockStore.getInfo(blockId);

    Set<String> hosts = new HashSet<>();
    for (BlockLocation blockLocation : blockInfo.getLocations()) {
      hosts.add(blockLocation.getWorkerAddress().getHost());
    }
    Map<WorkerInfo, SerializableVoid> result = Maps.newHashMap();

    Collections.shuffle(jobWorkerInfoList);
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      // Select job workers that have this block locally to evict
      if (hosts.contains(workerInfo.getAddress().getHost())) {
        result.put(workerInfo, null);
        if (result.size() >= numReplicas) {
          break;
        }
      }
    }
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * This task will evict the given block.
   */
  @Override
  public SerializableVoid runTask(EvictConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext) throws Exception {
    AlluxioBlockStore blockStore = AlluxioBlockStore.create();

    long blockId = config.getBlockId();
    String localHostName = NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC);
    List<BlockWorkerInfo> workerInfoList = blockStore.getAllWorkers();
    WorkerNetAddress localNetAddress = null;

    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getNetAddress().getHost().equals(localHostName)) {
        localNetAddress = workerInfo.getNetAddress();
        break;
      }
    }
    if (localNetAddress == null) {
      String message = String.format("Cannot find a local block worker to evict block %d", blockId);
      throw new NotFoundException(message);
    }

    RemoveBlockRequest request = RemoveBlockRequest.newBuilder().setBlockId(blockId).build();
    try (BlockWorkerClient blockWorker =
             FileSystemContext.get().acquireBlockWorkerClient(localNetAddress)) {
      blockWorker.removeBlock(request);
    } catch (NotFoundException e) {
      // Instead of throwing this exception, we continue here because the block to evict does not
      // exist on this worker anyway.
      LOG.warn("Failed to delete block {} on {}: block does not exist", blockId, localNetAddress);
    }
    return null;
  }
}
