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
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.HeartbeatThread;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.job.Job;
import tachyon.master.MasterBase;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.lineage.checkpoint.CheckpointManager;
import tachyon.master.lineage.checkpoint.CheckpointPlanningExecutor;
import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.lineage.meta.LineageStore;
import tachyon.master.lineage.recompute.RecomputeExecutor;
import tachyon.master.lineage.recompute.RecomputePlanner;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.CheckpointFile;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
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
  private final FileSystemMaster mFileSystemMaster;
  private final CheckpointManager mCheckpointManager;

  /** The service that checkpoints lineages. */
  private Future<?> mCheckpointExecutionService;
  /** The service that recomputes lineages. */
  private Future<?> mRecomputeExecutionService;

  public LineageMaster(TachyonConf conf, Journal journal, FileSystemMaster fileSystemMaster) {
    super(journal,
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("file-system-master-%d", true)));

    mTachyonConf = Preconditions.checkNotNull(conf);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mLineageStore = new LineageStore();
    mCheckpointManager = new CheckpointManager(mLineageStore, mFileSystemMaster);
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
              new CheckpointPlanningExecutor(mTachyonConf, mCheckpointManager),
              mTachyonConf.getInt(Constants.MASTER_CHECKPOINT_INTERVAL_MS)));
      mRecomputeExecutionService =
          getExecutorService().submit(new HeartbeatThread("Recomupte service",
              new RecomputeExecutor(new RecomputePlanner(mLineageStore, mFileSystemMaster)),
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

  public long createLineage(List<TachyonURI> inputFiles, List<TachyonURI> outputFiles, Job job) {
    // validate input files exist
    List<TachyonFile> inputTachyonFiles = Lists.newArrayList();
    for (TachyonURI inputFile : inputFiles) {
      long fileId;
      try {
        fileId = mFileSystemMaster.getFileId(inputFile);
        inputTachyonFiles.add(new TachyonFile(fileId));
      } catch (InvalidPathException e) {
        // TODO error handling
      }
    }
    // create output files
    List<LineageFile> outputTachyonFiles = Lists.newArrayList();
    for (TachyonURI outputFile : outputFiles) {
      long fileId;
      try {
        fileId = mFileSystemMaster.createFile(outputFile, 0, true);
        outputTachyonFiles.add(new LineageFile(fileId));
      } catch (InvalidPathException e) {
        // TODO error handling
      } catch (FileAlreadyExistException e) {
        // TODO error handling
      } catch (BlockInfoException e) {
        // TODO error handling
      }
    }

    return mLineageStore.createLineage(inputTachyonFiles, outputTachyonFiles, job);
  }

  public boolean deleteLineage(long lineageId, boolean cascade) {
    Lineage lineage = mLineageStore.getLineage(lineageId);
    if (lineage == null) {
      // TODO error handling
    }

    // there should not be child lineage if cascade
    if (!cascade && !mLineageStore.getChildren(lineage).isEmpty()) {
      // TODO error handling
    }

    mLineageStore.deleteLineage(lineageId);
    return true;
  }

  public long recreateFile(String path, long blockSizeBytes) {
    try {
      return mFileSystemMaster.resetBlockSize(new TachyonURI(path), blockSizeBytes);
    } catch (InvalidPathException e) {
      // TODO(yupeng): error handling
      return -1;
    }
  }

  public void asyncCompleteFile(long fileId, String underFsPath) {
    mLineageStore.completeFileForAsyncWrite(fileId, underFsPath);
  }

  /**
   * Instructs a worker to persist the files for checkpoint.
   *
   * TODO(yupeng) run the heartbeat in a thread?
   *
   * @param workerId the id of the worker that heartbeats
   * @return the command for checkpointing the blocks of a file.
   */
  public LineageCommand lineageWorkerHeartbeat(long workerId, List<Long> persistedFiles) {
    // notify checkpoitn manager the persisted files
    mCheckpointManager.commitCheckpointFiles(workerId, persistedFiles);

    // get the files for the given worker to checkpoitn
    List<CheckpointFile> filesToCheckpoint = null;
    try {
      filesToCheckpoint = mCheckpointManager.getFilesToCheckpoint(workerId);
    } catch (FileDoesNotExistException e) {
      // TODO error handling
    }
    return new LineageCommand(CommandType.Persist, filesToCheckpoint);
  }

}
