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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.HeartbeatThread;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.job.Job;
import tachyon.master.MasterBase;
import tachyon.master.MasterContext;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.lineage.checkpoint.CheckpointManager;
import tachyon.master.lineage.checkpoint.CheckpointPlanningExecutor;
import tachyon.master.lineage.journal.AsyncCompleteFileEntry;
import tachyon.master.lineage.journal.LineageEntry;
import tachyon.master.lineage.journal.LineageIdGeneratorEntry;
import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.lineage.meta.LineageIdGenerator;
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
import tachyon.thrift.LineageInfo;
import tachyon.thrift.LineageMasterService;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.io.PathUtils;

/**
 * The lineage master stores the lineage metadata in Tachyon, and it contains the components that
 * manage all lineage-related activities.
 */
public final class LineageMaster extends MasterBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final TachyonConf mTachyonConf;
  private final LineageStore mLineageStore;
  private final FileSystemMaster mFileSystemMaster;
  private final CheckpointManager mCheckpointManager;
  private final LineageIdGenerator mLineageIdGenerator;

  /** The service that checkpoints lineages. */
  private Future<?> mCheckpointExecutionService;
  /** The service that recomputes lineages. */
  private Future<?> mRecomputeExecutionService;

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.LINEAGE_MASTER_SERVICE_NAME);
  }

  public LineageMaster(Journal journal, FileSystemMaster fileSystemMaster) {
    super(journal,
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("file-system-master-%d", true)));

    mTachyonConf = MasterContext.getConf();
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mLineageIdGenerator = new LineageIdGenerator();
    mLineageStore = new LineageStore(mLineageIdGenerator);
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
    if (entry instanceof LineageEntry) {
      mLineageStore.addLineageFromJournal((LineageEntry) entry);
    } else if (entry instanceof LineageIdGeneratorEntry) {
      mLineageIdGenerator.fromJournalEntry((LineageIdGeneratorEntry) entry);
    } else if (entry instanceof AsyncCompleteFileEntry) {
      asyncCompleteFileFromEntry((AsyncCompleteFileEntry) entry);
    } else {
      throw new IOException(ExceptionMessage.UNEXPECETD_JOURNAL_ENTRY.getMessage(entry));
    }
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
          getExecutorService()
              .submit(new HeartbeatThread("Recomupte service",
                  new RecomputeExecutor(new RecomputePlanner(mLineageStore, mFileSystemMaster),
                      mFileSystemMaster),
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
    mLineageStore.streamToJournalCheckpoint(outputStream);
  }

  public long createLineage(List<TachyonURI> inputFiles, List<TachyonURI> outputFiles, Job job)
      throws InvalidPathException, FileAlreadyExistException, BlockInfoException {
    // validate input files exist
    List<TachyonFile> inputTachyonFiles = Lists.newArrayList();
    for (TachyonURI inputFile : inputFiles) {
      long fileId;
      fileId = mFileSystemMaster.getFileId(inputFile);
      inputTachyonFiles.add(new TachyonFile(fileId));
    }
    // create output files
    List<LineageFile> outputTachyonFiles = Lists.newArrayList();
    for (TachyonURI outputFile : outputFiles) {
      long fileId;
      fileId = mFileSystemMaster.createFile(outputFile, 1, true);
      outputTachyonFiles.add(new LineageFile(fileId));
    }

    LOG.info("Create lineage of input:" + inputTachyonFiles + ", output:" + outputTachyonFiles
        + ", job:" + job);
    long lineageId = mLineageStore.createLineage(inputTachyonFiles, outputTachyonFiles, job);

    writeJournalEntry(mLineageIdGenerator.toJournalEntry());
    writeJournalEntry(mLineageStore.getLineage(lineageId).toJournalEntry());
    flushJournal();
    return lineageId;
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

    LOG.info("Delete lineage " + lineageId);
    mLineageStore.deleteLineage(lineageId);
    return true;
  }

  public long recreateFile(String path, long blockSizeBytes) throws InvalidPathException {
    LOG.info("Recreate the file " + path + " with block size of " + blockSizeBytes + " bytes");
    return mFileSystemMaster.resetBlockSize(new TachyonURI(path), blockSizeBytes);
  }

  public void asyncCompleteFile(long fileId, String underFsPath)
      throws FileDoesNotExistException, BlockInfoException {
    LOG.info("Asyn complete file " + fileId + " with under file system path " + underFsPath);
    mLineageStore.completeFileForAsyncWrite(fileId, underFsPath);
    // complete file in Tachyon.
    mFileSystemMaster.completeFile(fileId);
    writeJournalEntry(new AsyncCompleteFileEntry(fileId, underFsPath));
    flushJournal();
  }

  private void asyncCompleteFileFromEntry(AsyncCompleteFileEntry entry) {
    mLineageStore.completeFileForAsyncWrite(entry.getFileId(), entry.getUnderFsPath());
  }

  /**
   * Instructs a worker to persist the files for checkpoint.
   *
   * TODO(yupeng) run the heartbeat in a thread?
   *
   * @param workerId the id of the worker that heartbeats
   * @return the command for checkpointing the blocks of a file.
   * @throws FileDoesNotExistException
   */
  public LineageCommand lineageWorkerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException {
    // notify checkpoitn manager the persisted files
    mCheckpointManager.commitCheckpointFiles(workerId, persistedFiles);

    // get the files for the given worker to checkpoint
    List<CheckpointFile> filesToCheckpoint = null;
    filesToCheckpoint = mCheckpointManager.getFilesToCheckpoint(workerId);
    LOG.info("Sent files " + filesToCheckpoint + " to worker " + workerId + " to persist");
    return new LineageCommand(CommandType.Persist, filesToCheckpoint);
  }

  public List<LineageInfo> listLineages() {
    List<LineageInfo> lineages = Lists.newArrayList();

    for (Lineage lineage : mLineageStore.getAllInTopologicalOrder()) {
      LineageInfo info = lineage.generateLineageInfo();
      List<Long> parents = Lists.newArrayList();
      for (Lineage parent : mLineageStore.getParents(lineage)) {
        parents.add(parent.getId());
      }
      info.parents = parents;
      List<Long> children = Lists.newArrayList();
      for (Lineage child : mLineageStore.getChildren(lineage)) {
        children.add(child.getId());
      }
      info.children = children;
      lineages.add(info);
    }
    LOG.info("List the lineage infos" + lineages);
    return lineages;
  }
}
