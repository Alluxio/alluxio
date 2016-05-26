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

package alluxio.worker.file;

import alluxio.Constants;
import alluxio.exception.ConnectionFailedException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.thrift.CommandType;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.PersistFile;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.WorkerContext;
import alluxio.worker.WorkerIdRegistry;
import alluxio.worker.block.BlockMasterSync;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class that communicates to file system master via heartbeat. This class manages its own
 * {@link FileSystemMasterClient}.
 *
 * When running, this class pulls from the master to check which file to persist for async
 * persistence.
 *
 * If the task fails to heartbeat to the master, it will destroy its old master client and recreate
 * it before retrying.
 *
 * TODO(yupeng): merge this with {@link BlockMasterSync} to use a central command pattern.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
final class FileWorkerMasterSyncExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Logic for managing async file persistence. */
  private final FileDataManager mFileDataManager;
  /** Client for communicating to file system master. */
  private final FileSystemMasterClient mMasterClient;
  /** The thread pool to persist file. */
  private final ExecutorService mPersistFileService;

  /**
   * Creates a new instance of {@link FileWorkerMasterSyncExecutor}.
   *
   * @param fileDataManager a {@link FileDataManager} handle
   * @param masterClient a {@link FileSystemMasterClient}
   */
  public FileWorkerMasterSyncExecutor(FileDataManager fileDataManager,
      FileSystemMasterClient masterClient) {
    mFileDataManager = Preconditions.checkNotNull(fileDataManager);
    mMasterClient = Preconditions.checkNotNull(masterClient);
    mPersistFileService = Executors.newFixedThreadPool(
        WorkerContext.getConf().getInt(Constants.WORKER_FILE_PERSIST_POOL_SIZE),
        ThreadFactoryUtils.build("persist-file-service-%d", true));
  }

  @Override
  public void heartbeat() {
    List<Long> persistedFiles = mFileDataManager.getPersistedFiles();
    if (!persistedFiles.isEmpty()) {
      LOG.info("files {} persisted", persistedFiles);
    }

    FileSystemCommand command;
    try {
      command = mMasterClient.heartbeat(WorkerIdRegistry.getWorkerId(), persistedFiles);
    } catch (IOException e) {
      LOG.error("Failed to heartbeat to master", e);
      return;
    } catch (ConnectionFailedException e) {
      LOG.error("Failed to heartbeat to master", e);
      return;
    }

    // removes the persisted files that are confirmed
    mFileDataManager.clearPersistedFiles(persistedFiles);

    if (command == null) {
      LOG.error("The command sent from master is null");
      return;
    } else if (command.getCommandType() != CommandType.Persist) {
      LOG.error("The command sent from master should be PERSIST type, but was {}",
          command.getCommandType());
      return;
    }

    for (PersistFile persistFile : command.getCommandOptions().getPersistOptions()
            .getPersistFiles()) {
      long fileId = persistFile.getFileId();
      if (mFileDataManager.needPersistence(fileId)) {
        // lock all the blocks of the file to prevent eviction
        try {
          mFileDataManager.lockBlocks(fileId, persistFile.getBlockIds());
        } catch (IOException e) {
          LOG.error("Failed to lock the blocks for file {}", fileId, e);
        }

        // enqueue the persist request
        mPersistFileService
            .execute(new FilePersister(mFileDataManager, fileId, persistFile.getBlockIds()));
      }
    }
  }

  @Override
  public void close() {
    mPersistFileService.shutdown();
  }

  /**
   * Thread to persist a file into under file system.
   */
  class FilePersister implements Runnable {
    private FileDataManager mFileDataManager;
    private long mFileId;
    private List<Long> mBlockIds;

    /**
     * Creates a new instance of {@link FilePersister}.
     *
     * @param fileDataManager a {@link FileDataManager} handle
     * @param fileId a file id
     * @param blockIds a list of block ids
     */
    public FilePersister(FileDataManager fileDataManager, long fileId, List<Long> blockIds) {
      mFileDataManager = fileDataManager;
      mFileId = fileId;
      mBlockIds = blockIds;
    }

    @Override
    public void run() {
      try {
        LOG.info("persist file {} of blocks {}", mFileId, mBlockIds);
        mFileDataManager.persistFile(mFileId, mBlockIds);
      } catch (IOException e) {
        LOG.error("Failed to persist file {}", mFileId, e);
      }
    }
  }
}
