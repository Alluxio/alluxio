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

package alluxio.job.plan.batch;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.AbstractVoidPlanDefinition;
import alluxio.job.plan.BatchedJobConfig;
import alluxio.job.plan.load.LoadConfig;
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
public final class BatchedJobDefinition
    extends PlanDefinition<BatchedJobConfig, ArrayList<BatchedJobTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchedJobDefinition.class);
  private static final int MAX_BUFFER_SIZE = 500 * Constants.MB;
  private static final int JOBS_PER_WORKER = 10;

  /**
   * Constructs a new {@link BatchedJobDefinition}.
   */
  public BatchedJobDefinition() {
  }

  @Override
  public Set<Pair<WorkerInfo, ArrayList<BatchedJobTask>>> selectExecutors(BatchedJobConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context)
      throws Exception {


    return null;
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
  public SerializableVoid runTask(BatchedJobConfig config, ArrayList<BatchedJobTask> tasks,
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
  public static class BatchedJobTask implements Serializable {
    private static final long serialVersionUID = 2028545900913354425L;
    final long mBlockId;

    /**
     * @param blockId the id of the block to load
     */
    public BatchedJobTask(long blockId) {
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
  public Class<BatchedJobConfig> getJobConfigClass() {
    return BatchedJobConfig.class;
  }
}
