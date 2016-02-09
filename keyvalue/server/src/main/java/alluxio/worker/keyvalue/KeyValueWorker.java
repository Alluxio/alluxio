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

package alluxio.worker.keyvalue;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.thrift.KeyValueWorkerClientService;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.WorkerBase;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;

import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A worker serving key-value queries.
 */
@ThreadSafe
public class KeyValueWorker extends WorkerBase {
  /** Configuration object. */
  private final Configuration mConfiguration;
  /** BlockWorker handle for access block info. */
  private final BlockWorker mBlockWorker;
  /** Logic for handling key-value RPC requests. */
  private final KeyValueWorkerClientServiceHandler mKeyValueServiceHandler;

  /**
   * Constructor of {@link KeyValueWorker}.
   *
   * @param blockWorker handler to the {@link BlockWorker}
   */
  public KeyValueWorker(BlockWorker blockWorker) {
    // TODO(binfan): figure out do we really need thread pool for key-value worker (and for what)
    super(Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("keyvalue-worker-heartbeat-%d", true)));
    mConfiguration = WorkerContext.getConf();
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mKeyValueServiceHandler = new KeyValueWorkerClientServiceHandler(mBlockWorker);
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
    // No heartbeat thread to start
    // Thrift service is multiplexed with other services and will be started together with others
  }

  @Override
  public void stop() throws IOException {
    // No heartbeat thread to stop
    // Thrift service is multiplexed with other services and will be stopped together with others
  }
}
