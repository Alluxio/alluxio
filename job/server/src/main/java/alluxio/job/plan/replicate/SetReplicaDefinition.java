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

package alluxio.job.plan.replicate;

import alluxio.AlluxioURI;
import alluxio.client.block.BlockStoreClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.AbstractVoidPlanDefinition;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.resource.CloseableResource;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job to replicate a block. This job is invoked by the checker of replication level in
 * FileSystemMaster.
 */
@NotThreadSafe
public final class SetReplicaDefinition
    extends AbstractVoidPlanDefinition<SetReplicaConfig, SetReplicaTask> {
  private static final Logger LOG = LoggerFactory.getLogger(SetReplicaDefinition.class);

  /**
   * Constructs a new {@link SetReplicaDefinition}.
   *
   */
  public SetReplicaDefinition() {}

  @Override
  public Class<SetReplicaConfig> getJobConfigClass() {
    return SetReplicaConfig.class;
  }

  @Override
  public Set<Pair<WorkerInfo, SetReplicaTask>> selectExecutors(SetReplicaConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context) throws Exception {
    Preconditions.checkArgument(!jobWorkerInfoList.isEmpty(), "No worker is available");

    long blockId = config.getBlockId();
    int numReplicas = config.getReplicas();
    Preconditions.checkArgument(numReplicas >= 0);

    BlockStoreClient blockStore = BlockStoreClient.create(context.getFsContext());
    BlockInfo blockInfo = blockStore.getInfo(blockId);
    int currentNumReplicas = blockInfo.getLocations().size();
    Set<Pair<WorkerInfo, SetReplicaTask>> result = Sets.newHashSet();
    if (numReplicas == currentNumReplicas) {
      LOG.warn("Evict target has already been satisfied for job:{}", config);
      return result;
    }

    int numToOperate = currentNumReplicas - numReplicas;
    Mode mode;
    if (numToOperate > 0) {
      mode = Mode.EVICT;
    } else {
      numToOperate = Math.abs(numToOperate);
      mode = Mode.REPLICATE;
    }

    Set<String> hosts = blockInfo.getLocations().stream().map(BlockLocation::getWorkerAddress)
        .map(WorkerNetAddress::getHost).collect(Collectors.toSet());

    Collections.shuffle(jobWorkerInfoList);
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      // Select job workers that have this block locally to evict
      boolean condition = hosts.contains(workerInfo.getAddress().getHost());
      // Select job workers that don't have this block locally to replicate
      if (mode == Mode.REPLICATE) {
        condition = !condition;
      }
      if (condition) {
        result.add(new Pair<>(workerInfo, new SetReplicaTask(mode)));
        if (result.size() >= numToOperate) {
          break;
        }
      }
    }
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * This task will set replica for the block.
   */
  @Override
  public SerializableVoid runTask(SetReplicaConfig config, SetReplicaTask task,
      RunTaskContext context) throws Exception {
    switch (task.getMode()) {
      case EVICT:
        evict(config, context);
        break;
      case REPLICATE:
        replicate(config, context);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unexpected replication mode {}.", task.getMode()));
    }
    return null;
  }

  private void evict(SetReplicaConfig config, RunTaskContext context) throws Exception {
    long blockId = config.getBlockId();
    String localHostName = NetworkAddressUtils
        .getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC, Configuration.global());
    List<BlockWorkerInfo> workerInfoList = context.getFsContext().getCachedWorkers();
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
    try (CloseableResource<BlockWorkerClient> blockWorker =
        context.getFsContext().acquireBlockWorkerClient(localNetAddress)) {
      blockWorker.get().removeBlock(request);
    } catch (NotFoundException e) {
      // Instead of throwing this exception, we continue here because the block to evict does not
      // exist on this worker anyway.
      LOG.warn("Failed to delete block {} on {}: block does not exist", blockId, localNetAddress);
    }
  }

  private void replicate(SetReplicaConfig config, RunTaskContext context) throws Exception {
    // TODO(jiri): Replace with internal client that uses file ID once the internal client is
    // factored out of the core server module. The reason to prefer using file ID for this job is
    // to avoid the the race between "replicate" and "rename", so that even a file to replicate is
    // renamed, the job is still working on the correct file.
    URIStatus status = context.getFileSystem().getStatus(new AlluxioURI(config.getPath()));

    JobUtils.loadBlock(status, context.getFsContext(), config.getBlockId(), null, false);
    LOG.info("Replicated file " + config.getPath() + " block " + config.getBlockId());
  }
}
