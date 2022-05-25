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

package alluxio.master.file.cmdmanager.task;

import alluxio.client.block.BlockStoreClient;
import alluxio.master.file.cmdmanager.command.CmdType;
import alluxio.master.file.cmdmanager.command.FsCmdConfig;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Class for create CmdRunStatus as task assignment.
 */
public class DefaultWorkerAssigner implements WorkerAssigner<RunStatus<BlockTask>> {
  private static final int BATCH_SIZE = 10;
  private AtomicInteger mID = new AtomicInteger(0);
  private final BlockStoreClient mClient; // todo: use correct clients to call worker api

  /**
   * Constructor.
   * @param client
   */
  public DefaultWorkerAssigner(BlockStoreClient client) {
    mClient = client;
  }

  @Override
  public Set<ExecutionWorkerInfo> getWorkers() {
    return null;
  }

  /**
   * Create a CmdRunStatus.
   * @param config command config
   * @param info worker info set
   * @return CmdRunStatus
   */
  @Override
  public RunStatus<BlockTask> createCommandAssignment(
          FsCmdConfig config, Set<ExecutionWorkerInfo> info) {
    if (config.getCmdType() == CmdType.LoadCmd) {
      List<BlockTask> blockTasks = createBlockTasks(config, info);
      return new LoadRunStatus(config, blockTasks);
    }
    throw new IllegalStateException("Unrecognized or unsupported type " + config.getCmdType());
  }

  // create list of BatchedBlockTask.
  private List<BlockTask> createBlockTasks(FsCmdConfig config, Set<ExecutionWorkerInfo> info) {
    List<Long> ids = getBlockIds(config);
    return Lists.partition(ids, BATCH_SIZE).stream()
            .map(t -> new BatchedBlockTask(mID.getAndIncrement(), BATCH_SIZE, t, mClient))
            .collect(Collectors.toList());
  }

  private List<Long> getBlockIds(FsCmdConfig config) {
    return null;
  }
}
