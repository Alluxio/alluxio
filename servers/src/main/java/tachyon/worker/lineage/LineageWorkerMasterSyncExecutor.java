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

package tachyon.worker.lineage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.client.lineage.LineageMasterClient;
import tachyon.thrift.CheckpointFile;
import tachyon.thrift.CommandType;
import tachyon.thrift.LineageCommand;
import tachyon.worker.block.BlockMasterSync;

/**
 * Class that communicates to lineage master via heartbeat. This class manages its own
 * {@link LineageMasterClient}.
 *
 * When running, this class pulls from the master to check which file to persist for lineage
 * checkpointing.
 *
 * If the task fails to heartbeat to the master, it will destroy its old master client and recreate
 * it before retrying.
 *
 * TODO(yupeng): merge this with {@link BlockMasterSync} to use a central command pattern.
 */
final class LineageWorkerMasterSyncExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int DEFAULT_FILE_PERSISTER_POOL_SIZE = 10;
  /** Logic for managing lineage file persistence */
  private final LineageDataManager mLineageDataManager;
  /** Client for communicating to lineage master */
  private final LineageMasterWorkerClient mMasterClient;
  /** The thread pool to persist file */
  private final ExecutorService mFixedExecutionService =
      Executors.newFixedThreadPool(DEFAULT_FILE_PERSISTER_POOL_SIZE);

  /** The id of the worker */
  private final long mWorkerId;

  public LineageWorkerMasterSyncExecutor(LineageDataManager lineageDataManager,
      LineageMasterWorkerClient masterClient, long workerId) {
    mWorkerId = workerId;
    mLineageDataManager = Preconditions.checkNotNull(lineageDataManager);
    mMasterClient = Preconditions.checkNotNull(masterClient);
  }

  @Override
  public void heartbeat() {
    List<Long> persistedFiles = mLineageDataManager.popPersistedFiles();
    if (!persistedFiles.isEmpty()) {
      LOG.info("files " + persistedFiles + " persisted");
    }

    LineageCommand command = null;
    try {
      command = mMasterClient.workerLineageHeartbeat(mWorkerId, persistedFiles);
    } catch (IOException e) {
      LOG.error("Failed to heartbeat to master", e);
    }
    Preconditions.checkState(command.commandType == CommandType.Persist);

    for (CheckpointFile checkpointFile : command.checkpointFiles) {
      mFixedExecutionService.execute(new FilePersister(mLineageDataManager, checkpointFile.fileId,
          checkpointFile.blockIds));
    }
  }

  /**
   * Thread to persist a file into under file system.
   */
  class FilePersister implements Runnable {
    private LineageDataManager mLineageDataManager;
    private long mFileId;
    private List<Long> mBlockIds;

    public FilePersister(LineageDataManager lineageDataManager, long fileId, List<Long> blockIds) {
      mLineageDataManager = lineageDataManager;
      mFileId = fileId;
      mBlockIds = blockIds;
    }

    @Override
    public void run() {
      try {
        LOG.info("persist file " + mFileId + " of blocks " + mBlockIds);
        mLineageDataManager.persistFile(mFileId, mBlockIds);
      } catch (IOException e) {
        LOG.error("Failed to persist file " + mFileId, e);
      }
    }
  }
}
