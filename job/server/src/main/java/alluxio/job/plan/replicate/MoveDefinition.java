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

import alluxio.collections.Pair;
import alluxio.conf.ServerConfiguration;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.MoveBlockRequest;
import alluxio.job.plan.AbstractVoidPlanDefinition;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.util.SerializableVoid;
import alluxio.resource.CloseableResource;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job to move a block. This job is invoked by the checker of replication level in
 * FileSystemMaster.
 */
@NotThreadSafe
public final class MoveDefinition
    extends AbstractVoidPlanDefinition<MoveConfig, SerializableVoid> {
  private static final Logger LOG = LoggerFactory.getLogger(MoveDefinition.class);

  /**
   * Constructs a new {@link MoveDefinition}.
   */
  public MoveDefinition() {
  }

  @Override
  public Class<MoveConfig> getJobConfigClass() {
    return MoveConfig.class;
  }

  @Override
  public Set<Pair<WorkerInfo, SerializableVoid>> selectExecutors(MoveConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context) {
    Preconditions.checkArgument(!jobWorkerInfoList.isEmpty(), "No worker is available");

    String workerHost = config.getWorkerHost();

    Set<Pair<WorkerInfo, SerializableVoid>> result = Sets.newHashSet();

    Collections.shuffle(jobWorkerInfoList);
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      // Select job workers that have this block locally to move
      if (workerHost.equals(workerInfo.getAddress().getHost())) {
        result.add(new Pair<>(workerInfo, null));
        return result;
      }
    }
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * This task will move the given block.
   */
  @Override
  public SerializableVoid runTask(MoveConfig config, SerializableVoid args, RunTaskContext context)
      throws Exception {
    long blockId = config.getBlockId();
    String localHostName = NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC,
        ServerConfiguration.global());
    List<BlockWorkerInfo> workerInfoList = context.getFsContext().getCachedWorkers();
    WorkerNetAddress localNetAddress = null;

    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getNetAddress().getHost().equals(localHostName)) {
        localNetAddress = workerInfo.getNetAddress();
        break;
      }
    }
    if (localNetAddress == null) {
      String message = String.format("Cannot find a local block worker to move block %d", blockId);
      throw new NotFoundException(message);
    }

    MoveBlockRequest request = MoveBlockRequest.newBuilder().setBlockId(blockId)
        .setMediumType(config.getMediumType()).build();
    try (CloseableResource<BlockWorkerClient> blockWorker =
             context.getFsContext().acquireBlockWorkerClient(localNetAddress)) {
      blockWorker.get().moveBlock(request);
    }
    return null;
  }
}

