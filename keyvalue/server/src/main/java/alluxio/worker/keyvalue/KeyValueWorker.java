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

package alluxio.worker.keyvalue;

import alluxio.Constants;
import alluxio.thrift.KeyValueWorkerClientService;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.AbstractWorker;
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
public final class KeyValueWorker extends AbstractWorker {
  /** BlockWorker handle for accessing block info. */
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
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mKeyValueServiceHandler = new KeyValueWorkerClientServiceHandler(mBlockWorker);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.KEY_VALUE_WORKER_CLIENT_SERVICE_NAME,
        new KeyValueWorkerClientService.Processor<>(mKeyValueServiceHandler));
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
