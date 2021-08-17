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

package alluxio.job.plan.load;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.job.plan.AbstractVoidPlanDefinition;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.load.LoadDefinition.LoadTask;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.util.CommonUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.TieredIdentity.LocalityTier;
import alluxio.wire.WorkerInfo;

import com.google.common.base.MoreObjects;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple loading job that loads the blocks of a file in a distributed and round-robin fashion.
 */
@NotThreadSafe
public final class LoadDefinition
    extends AbstractVoidPlanDefinition<LoadConfig, ArrayList<LoadTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(LoadDefinition.class);
  private static final int MAX_BUFFER_SIZE = 500 * Constants.MB;
  private static final int JOBS_PER_WORKER = 10;

  /**
   * Constructs a new {@link LoadDefinition}.
   */
  public LoadDefinition() {
  }

  @Override
  public Set<Pair<WorkerInfo, ArrayList<LoadTask>>> selectExecutors(LoadConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context)
      throws Exception {
    Map<String, WorkerInfo> jobWorkersByAddress = jobWorkerInfoList.stream()
        .collect(Collectors.toMap(info -> info.getAddress().getHost(), info -> info));
    // Filter out workers which have no local job worker available.
    List<String> missingJobWorkerHosts = new ArrayList<>();
    List<BlockWorkerInfo> workers = new ArrayList<>();
    for (BlockWorkerInfo worker : context.getFsContext().getCachedWorkers()) {
      if (jobWorkersByAddress.containsKey(worker.getNetAddress().getHost())) {
        String workerHost = worker.getNetAddress().getHost().toUpperCase();
        if (!isEmptySet(config.getExcludedWorkerSet())
            && config.getExcludedWorkerSet().contains(workerHost)) {
          continue;
        }
        // If specified the locality id, the candidate worker must match one at least
        boolean match = false;
        if (worker.getNetAddress().getTieredIdentity().getTiers() != null) {
          if (!(isEmptySet(config.getLocalityIds())
              && isEmptySet(config.getExcludedLocalityIds()))) {
            boolean exclude = false;
            for (LocalityTier tier : worker.getNetAddress().getTieredIdentity().getTiers()) {
              if (!isEmptySet(config.getExcludedLocalityIds())
                  && config.getExcludedLocalityIds().contains(tier.getValue().toUpperCase())) {
                exclude = true;
                break;
              }
              if (!isEmptySet(config.getLocalityIds())
                  && config.getLocalityIds().contains(tier.getValue().toUpperCase())) {
                match = true;
                break;
              }
            }
            if (exclude) {
              continue;
            }
          }
        }
        // Add current worker as candidate worker
        // if it matches the given locality id or contained in the given worker set
        // Or user specified neither worker-set nor locality id
        if ((isEmptySet(config.getWorkerSet()) && isEmptySet(config.getLocalityIds()))
            || match
            || (!isEmptySet(config.getWorkerSet())
                && config.getWorkerSet().contains(workerHost))) {
          workers.add(worker);
        }
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

    Set<Pair<WorkerInfo, ArrayList<LoadTask>>> result = Sets.newHashSet();
    for (Map.Entry<WorkerInfo, Collection<LoadTask>> assignment : assignments.asMap().entrySet()) {
      Collection<LoadTask> loadTasks = assignment.getValue();
      List<List<LoadTask>> partitionedTasks =
          CommonUtils.partition(Lists.newArrayList(loadTasks), JOBS_PER_WORKER);

      for (List<LoadTask> tasks : partitionedTasks) {
        if (!tasks.isEmpty()) {
          result.add(new Pair<>(assignment.getKey(), Lists.newArrayList(tasks)));
        }
      }
    }

    return result;
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

    // TODO(jiri): Replace with internal client that uses file ID once the internal client is
    // factored out of the core server module. The reason to prefer using file ID for this job is
    // to avoid the the race between "replicate" and "rename", so that even a file to replicate is
    // renamed, the job is still working on the correct file.
    URIStatus status = context.getFileSystem().getStatus(new AlluxioURI(config.getFilePath()));

    for (LoadTask task : tasks) {
      JobUtils.loadBlock(status, context.getFsContext(), task.getBlockId());
      LOG.info("Loaded file " + config.getFilePath() + " block " + task.getBlockId());
    }
    return null;
  }

  private boolean isEmptySet(Set s) {
    return s == null || s.isEmpty();
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
