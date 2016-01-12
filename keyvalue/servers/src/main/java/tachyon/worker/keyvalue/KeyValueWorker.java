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

package tachyon.worker.keyvalue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.thrift.TProcessor;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.KeyValueWorkerClientService;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.WorkerBase;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockDataManager;

/**
 * A worker serving KeyValue queries. This
 */
public class KeyValueWorker extends WorkerBase {
  /** Configuration object */
  private final TachyonConf mTachyonConf;
  /** Block data manager for access block info */
  private final BlockDataManager mBlockDataManager;
  /** Logic for handling key-value RPC requests. */
  private final KeyValueWorkerClientServiceHandler mKeyValueServiceHandler;

  /**
   * Constructor of KeyValueWorker.
   *
   * @param blockDataManager handler to the {@link BlockDataManager}
   */
  public KeyValueWorker(BlockDataManager blockDataManager) {
    // TODO(binfan): figure out do we really need thread pool for key-value worker (and for what)
    super(Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("keyvalue-worker-heartbeat-%d", true)));
    mTachyonConf = WorkerContext.getConf();
    mBlockDataManager = Preconditions.checkNotNull(blockDataManager);
    mKeyValueServiceHandler = new KeyValueWorkerClientServiceHandler(mBlockDataManager);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<String, TProcessor>();
    services.put(
        Constants.KEY_VALUE_WORKER_CLIENT_SERVICE_NAME,
        new KeyValueWorkerClientService.Processor<KeyValueWorkerClientServiceHandler>(
            mKeyValueServiceHandler));
    return services;
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }
}
