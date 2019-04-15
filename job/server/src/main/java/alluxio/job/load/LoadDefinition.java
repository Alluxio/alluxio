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

package alluxio.job.load;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.load.LoadDefinition.LoadTask;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.job.util.SerializationUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.base.MoreObjects;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple loading job that loads the blocks of a file in a distributed and round-robin fashion.
 */
@NotThreadSafe
public final class LoadDefinition
    extends AbstractVoidJobDefinition<LoadConfig, ArrayList<LoadTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(LoadDefinition.class);
  private static final int MAX_BUFFER_SIZE = 500 * Constants.MB;

  /**
   * Constructs a new {@link LoadDefinition}.
   */
  public LoadDefinition() {
  }

  @Override
  public Map<WorkerInfo, ArrayList<LoadTask>> selectExecutors(LoadConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context)
      throws Exception {
    Map<String, WorkerInfo> jobWorkersByAddress = jobWorkerInfoList.stream()
        .collect(Collectors.toMap(info -> info.getAddress().getHost(), info -> info));
    // Filter out workers which have no local job worker available.
    List<String> missingJobWorkerHosts = new ArrayList<>();
    List<BlockWorkerInfo> workers = new ArrayList<>();
    for (BlockWorkerInfo worker :
        AlluxioBlockStore.create(context.getFsContext()).getAllWorkers()) {
      if (jobWorkersByAddress.containsKey(worker.getNetAddress().getHost())) {
        workers.add(worker);
      } else {
        LOG.warn("Worker on host {} has no local job worker", worker.getNetAddress().getHost());
        missingJobWorkerHosts.add(worker.getNetAddress().getHost());
      }
    }
    // Mapping from worker to block ids which that worker is supposed to load.
    Multimap<WorkerInfo, LoadTask> assignments = LinkedListMultimap.create();
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    for (FileBlockInfo blockInfo : context.getFileSystem().getStatus(uri).getFileBlockInfos()) {
      List<String> workersWithoutBlock = getWorkersWithoutBlock(workers, blockInfo);
      int neededReplicas = config.getReplication() - blockInfo.getBlockInfo().getLocations().size();
      if (workersWithoutBlock.size() < neededReplicas) {
        String missingJobWorkersMessage = "";
        if (!missingJobWorkerHosts.isEmpty()) {
          missingJobWorkersMessage = ". The following workers could not be used because they have "
              + "no local job workers: " + missingJobWorkerHosts;
        }
        throw new FailedPreconditionException(String.format(
            "Failed to find enough block workers to replicate to. Needed %s but only found %s. "
            + "Available workers without the block: %s" + missingJobWorkersMessage,
            neededReplicas, workersWithoutBlock.size(), workersWithoutBlock));
      }
      Collections.shuffle(workersWithoutBlock);
      for (int i = 0; i < neededReplicas; i++) {
        String address = workersWithoutBlock.get(i);
        WorkerInfo jobWorker = jobWorkersByAddress.get(address);
        assignments.put(jobWorker, new LoadTask(blockInfo.getBlockInfo().getBlockId()));
      }
    }
    return SerializationUtils.makeValuesSerializable(assignments.asMap());
  }

  /**
   * @param blockWorkers a list of block workers
   * @param blockInfo information about a block
   * @return the block worker hosts which are not storing the specified block
   */
  private List<String> getWorkersWithoutBlock(List<BlockWorkerInfo> blockWorkers,
      FileBlockInfo blockInfo) {
    List<String> blockLocations = blockInfo.getBlockInfo().getLocations().stream()
        .map(location -> location.getWorkerAddress().getHost())
        .collect(Collectors.toList());
    return blockWorkers.stream()
        .filter(worker -> !blockLocations.contains(worker.getNetAddress().getHost()))
        .map(worker -> worker.getNetAddress().getHost())
        .collect(Collectors.toList());
  }

  @Override
  public SerializableVoid runTask(LoadConfig config, ArrayList<LoadTask> tasks,
      RunTaskContext context) throws Exception {
    for (LoadTask task : tasks) {
      JobUtils.loadBlock(
          context.getFileSystem(), context.getFsContext(), config.getFilePath(), task.getBlockId());
      LOG.info("Loaded block " + task.getBlockId());
    }
    return null;
  }

  /**
   * A task representing loading a block into the memory of a worker.
   */
  public static class LoadTask implements Serializable {
    private static final long serialVersionUID = 2028545900913354425L;
    final long mBlockId;

    /**
     * @param blockId the id of the block to load
     */
    public LoadTask(long blockId) {
      mBlockId = blockId;
    }

    /**
     * @return the block id
     */
    public long getBlockId() {
      return mBlockId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("blockId", mBlockId)
          .toString();
    }
  }

  @Override
  public Class<LoadConfig> getJobConfigClass() {
    return LoadConfig.class;
  }
}
