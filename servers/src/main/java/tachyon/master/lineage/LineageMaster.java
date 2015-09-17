/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.lineage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.thrift.TProcessor;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.HeartbeatThread;
import tachyon.client.file.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.job.Job;
import tachyon.master.MasterBase;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.lineage.checkpoint.CheckpointPlanningExecutor;
import tachyon.master.lineage.meta.LineageStore;
import tachyon.master.lineage.recompute.RecomputeExecutor;
import tachyon.master.lineage.recompute.RecomputeLauncher;
import tachyon.master.lineage.recompute.RecomputePlanner;
import tachyon.thrift.LineageCommand;
import tachyon.thrift.LineageMasterService;
import tachyon.util.ThreadFactoryUtils;

/**
 * The lineage master stores the lineage metadata in Tachyon, and it contains the components that
 * manage all lineage-related activities.
 */
public final class LineageMaster extends MasterBase {
  private final TachyonConf mTachyonConf;
  private final LineageStore mLineageStore;

  /** The service that checkpoints lineages. */
  private Future<?> mCheckpointExecutionService;
  /** The service that recomputes lineages. */
  private Future<?> mRecomputeExecutionService;


  public LineageMaster(TachyonConf conf, Journal journal) {
    super(journal,
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("file-system-master-%d", true)));

    mTachyonConf = Preconditions.checkNotNull(conf);
    mLineageStore = new LineageStore();
  }

  @Override
  public TProcessor getProcessor() {
    return new LineageMasterService.Processor<LineageMasterServiceHandler>(
        new LineageMasterServiceHandler(this));
  }

  @Override
  public String getServiceName() {
    return Constants.LINEAGE_MASTER_SERVICE_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // TODO add journal support
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      mCheckpointExecutionService =
          getExecutorService().submit(new HeartbeatThread("Checkpoint planning service",
              new CheckpointPlanningExecutor(mTachyonConf, mLineageStore),
              mTachyonConf.getInt(Constants.MASTER_CHECKPOINT_INTERVAL_MS)));
      mRecomputeExecutionService =
          getExecutorService().submit(new HeartbeatThread("Recomupte service",
              new RecomputeExecutor(new RecomputePlanner(mLineageStore),
                  new RecomputeLauncher(mTachyonConf, mLineageStore)),
              mTachyonConf.getInt(Constants.MASTER_RECOMPUTE_INTERVAL_MS)));
    }
  }

  @Override
  public void stop() throws IOException {
    super.stop();
    if (mCheckpointExecutionService != null) {
      mCheckpointExecutionService.cancel(true);
    }
    if (mRecomputeExecutionService != null) {
      mRecomputeExecutionService.cancel(true);
    }
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    // TODO add journal support
  }

  public long createLineage(List<TachyonFile> inputFiles, List<TachyonFile> outputFiles, Job job) {
    return mLineageStore.addLineage(inputFiles, outputFiles, job);
  }

  public boolean deleteLineage(long lineageId) {
    // TODO delete lineage
    return false;
  }

  /**
   * Instructs a worker to persist the files for checkpoint.
   *
   * @param workerId the id of the worker that heartbeats
   * @return the command for checkpointing the blocks of a file.
   */
  public LineageCommand lineageWorkerHeartbeat(long workerId) {
    // TODO
    return null;
  }

}
