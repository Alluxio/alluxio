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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.client.lineage.LineageMasterClient;
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

  /** Logic for managing lineage file persistence */
  private final LineageDataManager mLineageDataManager;
  /** Client for communicating to lineage master */
  private final LineageMasterWorkerClient mMasterClient;

  /** The id of the worker */
  private long mWorkerId;

  public LineageWorkerMasterSyncExecutor(LineageDataManager lineageDataManager,
      LineageMasterWorkerClient masterClient) {
    mLineageDataManager = Preconditions.checkNotNull(lineageDataManager);
    mMasterClient = Preconditions.checkNotNull(masterClient);
    mWorkerId = 0;
  }

  @Override
  public void heartbeat() {
    // TODO: get worker id
    LineageCommand command = mMasterClient.lineageWorkerHeartbeat(mWorkerId);
    Preconditions.checkState(command.mCommandType == CommandType.Persiste);
    mLineageDataManager.persistFile(command.mBlockIds, command.mFilePath);
  }

}
