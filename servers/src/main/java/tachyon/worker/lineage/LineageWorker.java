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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.HeartbeatThread;
import tachyon.conf.TachyonConf;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockDataManager;

/**
 * This class is responsible for managing all top level components of the lineage worker.
 */
public final class LineageWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Logic for managing lineage file persistence */
  private final LineageDataManager mLineageDataManager;
  /** Threadpool for the lineage master sync */
  /** The executor service for the master client thread */
  private final ExecutorService mMasterClientExecutorService;
  /** The executor service for the master sync */
  private final ExecutorService mSyncExecutorService;
  /** Client for lineage master communication. */
  private final LineageMasterWorkerClient mLineageMasterWorkerClient;
  /** Configuration object */
  private final TachyonConf mTachyonConf;
  private final long mWorkerId;

  /** The service that persists files for lineage checkpointing */
  private Future<?> mFilePersistenceService;

  public LineageWorker(BlockDataManager blockDataManager, long workerId) {
    Preconditions.checkState(workerId != 0, "Failed to register worker");

    mTachyonConf = WorkerContext.getConf();
    mLineageDataManager =
        new LineageDataManager(Preconditions.checkNotNull(blockDataManager));
    mWorkerId = workerId;

    // Setup MasterClientBase along with its heartbeat ExecutorService
    mMasterClientExecutorService = Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("lineage-worker-client-heartbeat-%d", true));
    mLineageMasterWorkerClient = new LineageMasterWorkerClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mTachyonConf),
        mMasterClientExecutorService, mTachyonConf);

    mSyncExecutorService = Executors.newFixedThreadPool(3,
        ThreadFactoryUtils.build("lineage-worker-heartbeat-%d", true));
  }

  public void start() {
    mFilePersistenceService = mSyncExecutorService.submit(new HeartbeatThread(
        "Lineage worker master sync", new LineageWorkerMasterSyncExecutor(mLineageDataManager,
            mLineageMasterWorkerClient, mWorkerId),
        mTachyonConf.getInt(Constants.WORKER_LINEAGE_HEARTBEAT_INTERVAL_MS)));
  }

  public void stop() {
    if (mFilePersistenceService != null) {
      mFilePersistenceService.cancel(true);
    }
    mLineageMasterWorkerClient.close();
    mSyncExecutorService.shutdown();
  }
}
