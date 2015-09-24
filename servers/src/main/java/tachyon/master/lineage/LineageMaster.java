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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
import tachyon.master.lineage.checkpoint.CheckpointPlan;
import tachyon.master.lineage.checkpoint.CheckpointPlanningExecutor;
import tachyon.master.lineage.journal.AsyncCompleteFileEntry;
import tachyon.master.lineage.journal.LineageEntry;
import tachyon.master.lineage.journal.LineageIdGeneratorEntry;
import tachyon.master.lineage.journal.PersistFilesEntry;
import tachyon.master.lineage.journal.RequestFilePersistenceEntry;
import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.lineage.meta.LineageFileState;
import tachyon.master.lineage.meta.LineageIdGenerator;
import tachyon.master.lineage.meta.LineageStore;
import tachyon.master.lineage.meta.LineageStoreView;
import tachyon.master.lineage.recompute.RecomputeExecutor;
import tachyon.master.lineage.recompute.RecomputePlanner;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.CheckpointFile;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileBlockInfo;
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
  private final LineageIdGenerator mLineageIdGenerator;

  /** The service that checkpoints lineages. */
  private Future<?> mCheckpointExecutionService;
  /** The service that recomputes lineages. */
  private Future<?> mRecomputeExecutionService;

  /** Map from worker to the files to checkpoint on that worker. Used by checkpoint service. */
  private final Map<Long, Set<LineageFile>> mWorkerToCheckpointFile;

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
    mWorkerToCheckpointFile = Maps.newHashMap();
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
    } else if (entry instanceof PersistFilesEntry) {
      persistFilesFromEntry((PersistFilesEntry) entry);
    } else if (entry instanceof RequestFilePersistenceEntry) {
      requestFilePersistenceFromEntry((RequestFilePersistenceEntry) entry);
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
              new CheckpointPlanningExecutor(mTachyonConf, this),
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

  public LineageStoreView getLineageStoreView() {
    return new LineageStoreView(mLineageStore);
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
    mLineageStore.completeFile(fileId, underFsPath);
    // complete file in Tachyon.
    mFileSystemMaster.completeFile(fileId);
    writeJournalEntry(new AsyncCompleteFileEntry(fileId, underFsPath));
    flushJournal();
  }

  private void asyncCompleteFileFromEntry(AsyncCompleteFileEntry entry) {
    mLineageStore.completeFile(entry.getFileId(), entry.getUnderFsPath());
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
    // notify checkpoint manager the persisted files
    persistFiles(workerId, persistedFiles);

    // get the files for the given worker to checkpoint
    List<CheckpointFile> filesToCheckpoint = null;
    filesToCheckpoint = pollToCheckpoint(workerId);
    if (!filesToCheckpoint.isEmpty()) {
      LOG.info("Sent files " + filesToCheckpoint + " to worker " + workerId + " to persist");
    }
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

  public synchronized void waitForCheckpoint(CheckpointPlan plan) {
    for (long lineageId : plan.getLineagesToCheckpoint()) {
      Lineage lineage = mLineageStore.getLineage(lineageId);
      // register the lineage file to checkpoint
      for (LineageFile file : lineage.getOutputFiles()) {
        // find the worker
        long workerId = findStoringWorker(file);
        if (workerId == -1) {
          // the file is not on any worker
          continue;
        }
        if (!mWorkerToCheckpointFile.containsKey(workerId)) {
          mWorkerToCheckpointFile.put(workerId, Sets.<LineageFile>newHashSet());
        }
        mWorkerToCheckpointFile.get(workerId).add(file);
      }
    }
  }

  /**
   * Polls the files to send to the given worker for checkpoint
   *
   * @param workerId the worker id
   * @return the list of files.
   * @throws FileDoesNotExistException
   */
  public synchronized List<CheckpointFile> pollToCheckpoint(long workerId)
      throws FileDoesNotExistException {
    List<CheckpointFile> files = Lists.newArrayList();
    if (!mWorkerToCheckpointFile.containsKey(workerId)) {
      return files;
    }

    List<Long> toRequestFilePersistence = Lists.newArrayList();
    for (LineageFile file : mWorkerToCheckpointFile.get(workerId)) {
      if (file.getState() == LineageFileState.COMPLETED) {
        long fileId = file.getFileId();
        toRequestFilePersistence.add(fileId);
        List<Long> blockIds = Lists.newArrayList();
        for (FileBlockInfo fileBlockInfo : mFileSystemMaster.getFileBlockInfoList(fileId)) {
          blockIds.add(fileBlockInfo.blockInfo.blockId);
        }
        String underFsPath = file.getUnderFilePath();

        CheckpointFile toCheckpoint = new CheckpointFile(fileId, blockIds, underFsPath);
        files.add(toCheckpoint);
      }
    }

    requestFilePersistence(toRequestFilePersistence);
    return files;
  }

  public synchronized void requestFilePersistence(List<Long> fileIds) {
    if (!fileIds.isEmpty()) {
      LOG.info("Request file persistency: " + fileIds);
    }
    for (long fileId : fileIds) {
      mLineageStore.requestFilePersistence(fileId);
    }
    writeJournalEntry(new RequestFilePersistenceEntry(fileIds));
    flushJournal();
  }

  private synchronized void requestFilePersistenceFromEntry(RequestFilePersistenceEntry entry) {
    for (long fileId : entry.getFileIds()) {
      mLineageStore.requestFilePersistence(fileId);
    }
  }

  public synchronized void persistFiles(long workerId, List<Long> persistedFiles) {
    Preconditions.checkNotNull(persistedFiles);

    if (!persistedFiles.isEmpty()) {
      LOG.info("Files persisted on worker " + workerId + ":" + persistedFiles);
    }
    for (Long fileId : persistedFiles) {
      mLineageStore.commitFilePersistence(fileId);
    }
    writeJournalEntry(new PersistFilesEntry(persistedFiles));
    flushJournal();
  }

  private synchronized void persistFilesFromEntry(PersistFilesEntry entry) {
    for (Long fileId : entry.getFileIds()) {
      mLineageStore.commitFilePersistence(fileId);
    }
  }

  private long findStoringWorker(LineageFile file) {
    List<Long> workers = Lists.newArrayList();
    try {
      for (FileBlockInfo fileBlockInfo : mFileSystemMaster.getFileBlockInfoList(file.getFileId())) {
        for (BlockLocation blockLocation : fileBlockInfo.blockInfo.locations) {
          workers.add(blockLocation.workerId);
        }
      }
    } catch (FileDoesNotExistException e) {
      // should not happen
      throw new RuntimeException(e);
    }

    if (workers.size() == 0) {
      LOG.info("the file " + file + " is not on any worker");
      return -1;
    }
    Preconditions.checkState(workers.size() < 2,
        "the file is stored at more than one worker: " + workers);
    return workers.get(0);
  }
}
