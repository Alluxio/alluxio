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

package tachyon.worker.file;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.exception.ConnectionFailedException;
import tachyon.heartbeat.HeartbeatExecutor;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileSystemCommand;
import tachyon.thrift.PersistFile;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.WorkerIdRegistry;
import tachyon.worker.block.BlockMasterSync;

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
final class FileWorkerMasterSyncExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int DEFAULT_FILE_PERSISTER_POOL_SIZE = 10;
  /** Logic for managing async file persistence */
  private final FileDataManager mFileDataManager;
  /** Client for communicating to file system master */
  private final FileSystemMasterClient mMasterClient;
  /** The thread pool to persist file */
  private final ExecutorService mPersistFileService = Executors.newFixedThreadPool(
      DEFAULT_FILE_PERSISTER_POOL_SIZE, ThreadFactoryUtils.build("persist-file-service-%d", true));

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
  }

  @Override
  public void heartbeat() {
    List<Long> persistedFiles = mFileDataManager.getPersistedFiles();
    if (!persistedFiles.isEmpty()) {
      LOG.info("files {} persisted", persistedFiles);
    }

    FileSystemCommand command = null;
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
    } else if (command.commandType != CommandType.Persist) {
      LOG.error("The command sent from master should be PERSIST type, but was {}",
          command.commandType);
      return;
    }

    for (PersistFile persistFile : command.getCommandOptions().getPersistOptions().persistFiles) {
      long fileId = persistFile.fileId;
      if (mFileDataManager.needPersistence(fileId)) {
        mPersistFileService
            .execute(new FilePersister(mFileDataManager, fileId, persistFile.blockIds));
      }
    }
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
