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
import alluxio.master.MasterRegistry;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.JournalOutputStream;
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
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.wire.FileInfo;
import alluxio.wire.LineageInfo;
import alluxio.wire.TtlAction;

import com.google.common.collect.ImmutableSet;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The lineage master stores the lineage metadata in Alluxio, and it contains the components that
 * manage all lineage-related activities.
 */
@NotThreadSafe
public final class LineageMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(LineageMaster.class);
  private static final Set<Class<?>> DEPS = ImmutableSet.<Class<?>>of(FileSystemMaster.class);

  private final FileSystemMaster mFileSystemMaster;
  private final LineageStore mLineageStore;
  private final LineageIdGenerator mLineageIdGenerator;

  /**
   * Creates a new instance of {@link LineageMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  public LineageMaster(MasterRegistry registry, JournalFactory journalFactory) {
    this(registry, journalFactory, ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.LINEAGE_MASTER_NAME, 2));
  }

  /**
   * Creates a new instance of {@link LineageMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  public LineageMaster(MasterRegistry registry, JournalFactory journalFactory,
      ExecutorServiceFactory executorServiceFactory) {
    super(journalFactory.create(Constants.LINEAGE_MASTER_NAME), new SystemClock(),
        executorServiceFactory);
    mLineageIdGenerator = new LineageIdGenerator();
    mLineageStore = new LineageStore(mLineageIdGenerator);
    mFileSystemMaster = registry.get(FileSystemMaster.class);
    registry.add(LineageMaster.class, this);
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
  public Set<Class<?>> getDependencies() {
    return DEPS;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
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
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      getExecutorService().submit(new HeartbeatThread(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING,
          new CheckpointSchedulingExecutor(this, mFileSystemMaster),
          Configuration.getInt(PropertyKey.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS)));
      getExecutorService().submit(new HeartbeatThread(HeartbeatContext.MASTER_FILE_RECOMPUTATION,
          new RecomputeExecutor(new RecomputePlanner(mLineageStore, mFileSystemMaster),
              mFileSystemMaster),
          Configuration.getInt(PropertyKey.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS)));
    }
  }

  @Override
  public synchronized void streamToJournalCheckpoint(JournalOutputStream outputStream)
      throws IOException {
    mLineageStore.streamToJournalCheckpoint(outputStream);
    outputStream.write(mLineageIdGenerator.toJournalEntry());
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
   * @throws IOException if the creation of a file fails
   * @throws AccessControlException if the permission check fails
   * @throws FileDoesNotExistException if any of the input files do not exist
   */
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
    } catch (LineageDoesNotExistException | LineageDeletionException e) {
      LOG.error("Failed to delete lineage {}", entry.getLineageId(), e);
    }
  }

  /**
   * Reinitializes the file when the file is lost or not completed.
   *
   * @param path the path to the file
   * @param blockSizeBytes the block size
   * @param ttl the TTL
   * @param ttlAction action to perform on ttl expiry
   * @return the id of the reinitialized file when the file is lost or not completed, -1 otherwise
   * @throws InvalidPathException the file path is invalid
   * @throws LineageDoesNotExistException when the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   */
  public synchronized long reinitializeFile(String path, long blockSizeBytes, long ttl,
      TtlAction ttlAction)
      throws InvalidPathException, LineageDoesNotExistException, AccessControlException,
      FileDoesNotExistException {
    long fileId = mFileSystemMaster.getFileId(new AlluxioURI(path));
    FileInfo fileInfo;
    try {
      fileInfo = mFileSystemMaster.getFileInfo(fileId);
      if (!fileInfo.isCompleted() || mFileSystemMaster.getLostFiles().contains(fileId)) {
        LOG.info("Recreate the file {} with block size of {} bytes", path, blockSizeBytes);
        return mFileSystemMaster
            .reinitializeFile(new AlluxioURI(path), blockSizeBytes, ttl, ttlAction);
      }
    } catch (FileDoesNotExistException e) {
      throw new LineageDoesNotExistException(
          ExceptionMessage.MISSING_REINITIALIZE_FILE.getMessage(path));
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

  /**
   * Schedules persistence for the output files of the given checkpoint plan.
   *
   * @param plan the plan for checkpointing
   */
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

  /**
   * Reports a file as lost.
   *
   * @param path the path to the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public synchronized void reportLostFile(String path) throws FileDoesNotExistException,
      AccessControlException, InvalidPathException {
    long fileId = mFileSystemMaster.getFileId(new AlluxioURI(path));
    mFileSystemMaster.reportLostFile(fileId);
  }
}
