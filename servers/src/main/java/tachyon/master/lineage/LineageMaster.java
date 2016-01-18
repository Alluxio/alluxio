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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.BlockInfoException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.LineageDeletionException;
import tachyon.exception.LineageDoesNotExistException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatThread;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.master.MasterBase;
import tachyon.master.MasterContext;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.file.options.CreateFileOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.journal.JournalProtoUtils;
import tachyon.master.lineage.checkpoint.CheckpointPlan;
import tachyon.master.lineage.checkpoint.CheckpointSchedulingExcecutor;
import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageIdGenerator;
import tachyon.master.lineage.meta.LineageStore;
import tachyon.master.lineage.meta.LineageStoreView;
import tachyon.master.lineage.recompute.RecomputeExecutor;
import tachyon.master.lineage.recompute.RecomputePlanner;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.proto.journal.Lineage.DeleteLineageEntry;
import tachyon.proto.journal.Lineage.LineageEntry;
import tachyon.proto.journal.Lineage.LineageIdGeneratorEntry;
import tachyon.thrift.FileInfo;
import tachyon.thrift.LineageInfo;
import tachyon.thrift.LineageMasterClientService;
import tachyon.util.IdUtils;
import tachyon.util.io.PathUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

  /**
   * The service that checkpoints lineages. We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mCheckpointExecutionService;
  /**
   * The service that recomputes lineages. We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mRecomputeExecutionService;

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.LINEAGE_MASTER_NAME);
  }

  /**
   * Creates a new instance of {@link LineageMaster}.
   *
   * @param fileSystemMaster the file system master
   * @param journal the journal
   */
  public LineageMaster(FileSystemMaster fileSystemMaster, Journal journal) {
    super(journal, 2);

    mTachyonConf = MasterContext.getConf();
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mLineageIdGenerator = new LineageIdGenerator();
    mLineageStore = new LineageStore(mLineageIdGenerator);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<String, TProcessor>();
    services.put(Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME,
        new LineageMasterClientService.Processor<LineageMasterClientServiceHandler>(
            new LineageMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.LINEAGE_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    Message innerEntry = JournalProtoUtils.unwrap(entry);
    if (innerEntry instanceof LineageEntry) {
      mLineageStore.addLineageFromJournal((LineageEntry) innerEntry);
    } else if (innerEntry instanceof LineageIdGeneratorEntry) {
      mLineageIdGenerator.initFromJournalEntry((LineageIdGeneratorEntry) innerEntry);
    } else if (innerEntry instanceof DeleteLineageEntry) {
      deleteLineageFromEntry((DeleteLineageEntry) innerEntry);
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(innerEntry));
    }
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      mCheckpointExecutionService = getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING,
              new CheckpointSchedulingExcecutor(this, mFileSystemMaster),
              mTachyonConf.getInt(Constants.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS)));
      mRecomputeExecutionService = getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.MASTER_FILE_RECOMPUTATION,
              new RecomputeExecutor(new RecomputePlanner(mLineageStore, mFileSystemMaster),
                  mFileSystemMaster),
              mTachyonConf.getInt(Constants.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS)));
    }
  }

  @Override
  public synchronized void streamToJournalCheckpoint(JournalOutputStream outputStream)
      throws IOException {
    mLineageStore.streamToJournalCheckpoint(outputStream);
    outputStream.writeEntry(mLineageIdGenerator.toJournalEntry());
  }

  /**
   * @return a lineage store view wrapping the contained lineage store
   */
  public LineageStoreView getLineageStoreView() {
    return new LineageStoreView(mLineageStore);
  }

  /**
   * Creates a lineage. It creates a new file for each output file.
   *
   * @param inputFiles the input files
   * @param outputFiles the output files
   * @param job the job
   * @return the id of the created lineage
   * @throws InvalidPathException if the path to the input file is invalid
   * @throws FileAlreadyExistsException if the output file already exists
   * @throws BlockInfoException if fails to create the output file
   */
  public synchronized long createLineage(List<TachyonURI> inputFiles, List<TachyonURI> outputFiles,
      Job job)
          throws InvalidPathException, FileAlreadyExistsException, BlockInfoException, IOException {
    List<Long> inputTachyonFiles = Lists.newArrayList();
    for (TachyonURI inputFile : inputFiles) {
      long fileId;
      fileId = mFileSystemMaster.getFileId(inputFile);
      if (fileId == IdUtils.INVALID_FILE_ID) {
        throw new InvalidPathException(
            ExceptionMessage.LINEAGE_INPUT_FILE_NOT_EXIST.getMessage(inputFile));
      }
      inputTachyonFiles.add(fileId);
    }
    // create output files
    List<Long> outputTachyonFiles = Lists.newArrayList();
    for (TachyonURI outputFile : outputFiles) {
      long fileId;
      // TODO(yupeng): delete the placeholder files if the creation fails.
      // Create the file initialized with block size 1KB as placeholder.
      CreateFileOptions options =
          new CreateFileOptions.Builder(MasterContext.getConf()).setRecursive(true)
              .setBlockSizeBytes(Constants.KB).build();
      fileId = mFileSystemMaster.create(outputFile, options);
      outputTachyonFiles.add(fileId);
    }

    LOG.info("Create lineage of input:{}, output:{}, job:{}", inputTachyonFiles, outputTachyonFiles,
        job);
    long lineageId = mLineageStore.createLineage(inputTachyonFiles, outputTachyonFiles, job);

    writeJournalEntry(mLineageIdGenerator.toJournalEntry());
    writeJournalEntry(mLineageStore.getLineage(lineageId).toJournalEntry());
    flushJournal();
    return lineageId;
  }

  /**
   * Deletes a lineage.
   *
   * @param lineageId id the of lineage
   * @param cascade the flag if to delete all the downstream lineages
   * @return true if the lineage is deleted, false otherwise
   * @throws LineageDoesNotExistException the lineage does not exist
   * @throws LineageDeletionException the lineage deletion fails
   */
  public synchronized boolean deleteLineage(long lineageId, boolean cascade)
      throws LineageDoesNotExistException, LineageDeletionException {
    deleteLineageInternal(lineageId, cascade);
    DeleteLineageEntry deleteLineage = DeleteLineageEntry.newBuilder()
        .setLineageId(lineageId)
        .setCascade(cascade)
        .build();
    writeJournalEntry(JournalEntry.newBuilder().setDeleteLineage(deleteLineage).build());
    flushJournal();
    return true;
  }

  private boolean deleteLineageInternal(long lineageId, boolean cascade)
      throws LineageDoesNotExistException, LineageDeletionException {
    Lineage lineage = mLineageStore.getLineage(lineageId);
    LineageDoesNotExistException.check(lineage != null, ExceptionMessage.LINEAGE_DOES_NOT_EXIST,
        lineageId);

    // there should not be child lineage if not cascade
    if (!cascade && !mLineageStore.getChildren(lineage).isEmpty()) {
      throw new LineageDeletionException(
          ExceptionMessage.DELETE_LINEAGE_WITH_CHILDREN.getMessage(lineageId));
    }

    LOG.info("Delete lineage {}", lineageId);
    mLineageStore.deleteLineage(lineageId);
    return true;
  }

  private void deleteLineageFromEntry(DeleteLineageEntry entry) {
    try {
      deleteLineageInternal(entry.getLineageId(), entry.getCascade());
    } catch (LineageDoesNotExistException e) {
      LOG.error("Failed to delete lineage {}", entry.getLineageId(), e);
    } catch (LineageDeletionException e) {
      LOG.error("Failed to delete lineage {}", entry.getLineageId(), e);
    }
  }

  /**
   * Reinitializes the file when the file is lost or not completed.
   *
   * @param path the path to the file
   * @param blockSizeBytes the block size
   * @param ttl the TTL
   * @return the id of the reinitialized file when the file is lost or not completed, -1 otherwise
   * @throws InvalidPathException the file path is invalid
   * @throws FileDoesNotExistException when the file does not exist
   */
  public synchronized long reinitializeFile(String path, long blockSizeBytes, long ttl)
      throws InvalidPathException, FileDoesNotExistException {
    long fileId = mFileSystemMaster.getFileId(new TachyonURI(path));
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    if (!fileInfo.isCompleted || mFileSystemMaster.getLostFiles().contains(fileId)) {
      LOG.info("Recreate the file {} with block size of {} bytes", path, blockSizeBytes);
      return mFileSystemMaster.reinitializeFile(new TachyonURI(path), blockSizeBytes, ttl);
    }
    return -1;
  }

  /**
   * @return the list of all the {@link LineageInfo}s
   * @throws LineageDoesNotExistException if the lineage does not exist
   * @throws FileDoesNotExistException if any associated file does not exist
   */
  public synchronized List<LineageInfo> getLineageInfoList()
      throws LineageDoesNotExistException, FileDoesNotExistException {
    List<LineageInfo> lineages = Lists.newArrayList();

    for (Lineage lineage : mLineageStore.getAllInTopologicalOrder()) {
      LineageInfo info = new LineageInfo();
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
      info.id = lineage.getId();
      List<String> inputFiles = Lists.newArrayList();
      for (long inputFileId : lineage.getInputFiles()) {
        inputFiles.add(mFileSystemMaster.getPath(inputFileId).toString());
      }
      info.inputFiles = inputFiles;
      List<String> outputFiles = Lists.newArrayList();
      for (long outputFileId : lineage.getOutputFiles()) {
        outputFiles.add(mFileSystemMaster.getPath(outputFileId).toString());
      }
      info.outputFiles = outputFiles;
      info.creationTimeMs = lineage.getCreationTime();
      info.job = ((CommandLineJob) lineage.getJob()).generateCommandLineJobInfo();

      lineages.add(info);
    }
    return lineages;
  }

  /**
   * Schedules persistence for the output files of the given checkpoint plan.
   *
   * @param plan the plan for checkpointing
   * @throws FileDoesNotExistException when a file doesn't exist
   */
  public synchronized void scheduleForCheckpoint(CheckpointPlan plan)
      throws FileDoesNotExistException {
    for (long lineageId : plan.getLineagesToCheckpoint()) {
      Lineage lineage = mLineageStore.getLineage(lineageId);
      // schedule the lineage file for persistence
      for (long file : lineage.getOutputFiles()) {
        mFileSystemMaster.scheduleAsyncPersistence(file);
      }
    }
  }

  /**
   * Polls the files to send to the given worker for checkpoint.
   *
   * @param path the path to the file
   * @throws FileDoesNotExistException if the file does not exist
   */
  public synchronized void reportLostFile(String path) throws FileDoesNotExistException {
    long fileId = mFileSystemMaster.getFileId(new TachyonURI(path));
    mFileSystemMaster.reportLostFile(fileId);
  }
}
