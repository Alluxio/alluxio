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

package alluxio.master.lineage;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.LineageDeletionException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.master.AbstractMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.master.lineage.checkpoint.CheckpointPlan;
import alluxio.master.lineage.checkpoint.CheckpointSchedulingExecutor;
import alluxio.master.lineage.meta.Lineage;
import alluxio.master.lineage.meta.LineageIdGenerator;
import alluxio.master.lineage.meta.LineageStore;
import alluxio.master.lineage.meta.LineageStoreView;
import alluxio.master.lineage.recompute.RecomputeExecutor;
import alluxio.master.lineage.recompute.RecomputePlanner;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Lineage.DeleteLineageEntry;
import alluxio.thrift.LineageMasterClientService;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.wire.FileInfo;
import alluxio.wire.LineageInfo;
import alluxio.wire.TtlAction;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The default lineage master stores the lineage metadata in Alluxio, and it contains the components
 * that manage all lineage-related activities.
 */
@NotThreadSafe
public final class DefaultLineageMaster extends AbstractMaster implements LineageMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultLineageMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(FileSystemMaster.class);

  private final FileSystemMaster mFileSystemMaster;
  private LineageStore mLineageStore;
  private final LineageIdGenerator mLineageIdGenerator;

  /**
   * Creates a new instance of {@link DefaultLineageMaster}.
   *
   * @param fileSystemMaster the file system master handle
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  DefaultLineageMaster(FileSystemMaster fileSystemMaster, JournalFactory journalFactory) {
    this(fileSystemMaster, journalFactory, ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.LINEAGE_MASTER_NAME, 2));
  }

  /**
   * Creates a new instance of {@link DefaultLineageMaster}.
   *
   * @param fileSystemMaster the file system master handle
   * @param journalFactory the factory for the journal to use for tracking master operations
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  DefaultLineageMaster(FileSystemMaster fileSystemMaster, JournalFactory journalFactory,
      ExecutorServiceFactory executorServiceFactory) {
    super(journalFactory.create(Constants.LINEAGE_MASTER_NAME), new SystemClock(),
        executorServiceFactory);
    mLineageIdGenerator = new LineageIdGenerator();
    mLineageStore = new LineageStore(mLineageIdGenerator);
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME,
        new LineageMasterClientService.Processor<>(new LineageMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.LINEAGE_MASTER_NAME;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    if (entry.getSequenceNumber() == 0) {
      mLineageStore = new LineageStore(mLineageIdGenerator);
    }
    if (entry.hasLineage()) {
      mLineageStore.addLineageFromJournal(entry.getLineage());
    } else if (entry.hasLineageIdGenerator()) {
      mLineageIdGenerator.initFromJournalEntry(entry.getLineageIdGenerator());
    } else if (entry.hasDeleteLineage()) {
      deleteLineageFromEntry(entry.getDeleteLineage());
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      getExecutorService().submit(new HeartbeatThread(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING,
          new CheckpointSchedulingExecutor(this, mFileSystemMaster),
          (int) Configuration.getMs(PropertyKey.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS)));
      getExecutorService().submit(new HeartbeatThread(HeartbeatContext.MASTER_FILE_RECOMPUTATION,
          new RecomputeExecutor(new RecomputePlanner(mLineageStore, mFileSystemMaster),
              mFileSystemMaster),
          (int) Configuration.getMs(PropertyKey.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS)));
    }
  }

  @Override
  public synchronized Iterator<JournalEntry> getJournalEntryIterator() {
    return Iterators.concat(mLineageStore.getJournalEntryIterator(),
        CommonUtils.singleElementIterator(mLineageIdGenerator.toJournalEntry()));
  }

  @Override
  public LineageStoreView getLineageStoreView() {
    return new LineageStoreView(mLineageStore);
  }

  @Override
  public synchronized long createLineage(List<AlluxioURI> inputFiles, List<AlluxioURI> outputFiles,
      Job job) throws InvalidPathException, FileAlreadyExistsException, BlockInfoException,
      IOException, AccessControlException, FileDoesNotExistException {
    List<Long> inputAlluxioFiles = new ArrayList<>();
    for (AlluxioURI inputFile : inputFiles) {
      long fileId;
      fileId = mFileSystemMaster.getFileId(inputFile);
      if (fileId == IdUtils.INVALID_FILE_ID) {
        throw new FileDoesNotExistException(
            ExceptionMessage.LINEAGE_INPUT_FILE_NOT_EXIST.getMessage(inputFile));
      }
      inputAlluxioFiles.add(fileId);
    }
    // create output files
    List<Long> outputAlluxioFiles = new ArrayList<>();
    for (AlluxioURI outputFile : outputFiles) {
      long fileId;
      // TODO(yupeng): delete the placeholder files if the creation fails.
      // Create the file initialized with block size 1KB as placeholder.
      CreateFileOptions options =
          CreateFileOptions.defaults().setRecursive(true).setBlockSizeBytes(Constants.KB);
      fileId = mFileSystemMaster.createFile(outputFile, options);
      outputAlluxioFiles.add(fileId);
    }

    LOG.info("Create lineage of input:{}, output:{}, job:{}", inputAlluxioFiles, outputAlluxioFiles,
        job);
    long lineageId = mLineageStore.createLineage(inputAlluxioFiles, outputAlluxioFiles, job);

    writeJournalEntry(mLineageIdGenerator.toJournalEntry());
    writeJournalEntry(mLineageStore.getLineage(lineageId).toJournalEntry());
    flushJournal();
    return lineageId;
  }

  @Override
  public synchronized boolean deleteLineage(long lineageId, boolean cascade)
      throws LineageDoesNotExistException, LineageDeletionException {
    deleteLineageInternal(lineageId, cascade);
    DeleteLineageEntry deleteLineage =
        DeleteLineageEntry.newBuilder().setLineageId(lineageId).setCascade(cascade).build();
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
    } catch (LineageDoesNotExistException | LineageDeletionException e) {
      LOG.error("Failed to delete lineage {}", entry.getLineageId(), e);
    }
  }

  @Override
  public synchronized long reinitializeFile(String path, long blockSizeBytes, long ttl,
      TtlAction ttlAction) throws InvalidPathException, LineageDoesNotExistException,
      AccessControlException, FileDoesNotExistException {
    long fileId = mFileSystemMaster.getFileId(new AlluxioURI(path));
    FileInfo fileInfo;
    try {
      fileInfo = mFileSystemMaster.getFileInfo(fileId);
      if (!fileInfo.isCompleted() || mFileSystemMaster.getLostFiles().contains(fileId)) {
        LOG.info("Recreate the file {} with block size of {} bytes", path, blockSizeBytes);
        return mFileSystemMaster.reinitializeFile(new AlluxioURI(path), blockSizeBytes, ttl,
            ttlAction);
      }
    } catch (FileDoesNotExistException e) {
      throw new LineageDoesNotExistException(
          ExceptionMessage.MISSING_REINITIALIZE_FILE.getMessage(path));
    }
    return -1;
  }

  @Override
  public synchronized List<LineageInfo> getLineageInfoList()
      throws LineageDoesNotExistException, FileDoesNotExistException {
    List<LineageInfo> lineages = new ArrayList<>();

    for (Lineage lineage : mLineageStore.getAllInTopologicalOrder()) {
      LineageInfo info = new LineageInfo();
      List<Long> parents = new ArrayList<>();
      for (Lineage parent : mLineageStore.getParents(lineage)) {
        parents.add(parent.getId());
      }
      info.setParents(parents);
      List<Long> children = new ArrayList<>();
      for (Lineage child : mLineageStore.getChildren(lineage)) {
        children.add(child.getId());
      }
      info.setChildren(children);
      info.setId(lineage.getId());
      List<String> inputFiles = new ArrayList<>();
      for (long inputFileId : lineage.getInputFiles()) {
        inputFiles.add(mFileSystemMaster.getPath(inputFileId).toString());
      }
      info.setInputFiles(inputFiles);
      List<String> outputFiles = new ArrayList<>();
      for (long outputFileId : lineage.getOutputFiles()) {
        outputFiles.add(mFileSystemMaster.getPath(outputFileId).toString());
      }
      info.setOutputFiles(outputFiles);
      info.setCreationTimeMs(lineage.getCreationTime());
      info.setJob(((CommandLineJob) lineage.getJob()).generateCommandLineJobInfo());

      lineages.add(info);
    }
    return lineages;
  }

  @Override
  public synchronized void scheduleCheckpoint(CheckpointPlan plan) {
    for (long lineageId : plan.getLineagesToCheckpoint()) {
      Lineage lineage = mLineageStore.getLineage(lineageId);
      // schedule the lineage file for persistence
      for (long file : lineage.getOutputFiles()) {
        try {
          mFileSystemMaster.scheduleAsyncPersistence(mFileSystemMaster.getPath(file));
        } catch (AlluxioException e) {
          LOG.error("Failed to persist the file {}.", file, e);
        }
      }
    }
  }

  @Override
  public synchronized void reportLostFile(String path)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    long fileId = mFileSystemMaster.getFileId(new AlluxioURI(path));
    mFileSystemMaster.reportLostFile(fileId);
  }
}
