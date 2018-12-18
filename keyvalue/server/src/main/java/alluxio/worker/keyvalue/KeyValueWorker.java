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
import alluxio.Server;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.block.BlockWorker;

import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A worker serving key-value queries.
 */
@ThreadSafe
public final class KeyValueWorker extends AbstractWorker {
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(BlockWorker.class);

  /** Logic for handling key-value RPC requests. */
  private final KeyValueWorkerClientServiceHandler mKeyValueServiceHandler;

  /**
   * Constructor of {@link KeyValueWorker}.
   *
   * @param blockWorker the block worker handle
   */
  KeyValueWorker(BlockWorker blockWorker) {
    // TODO(binfan): figure out do we really need thread pool for key-value worker (and for what)
    super(Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("keyvalue-worker-heartbeat-%d", true)));
    mKeyValueServiceHandler = new KeyValueWorkerClientServiceHandler(blockWorker);
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.KEY_VALUE_WORKER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(ServiceType.KEY_VALUE_WORKER_SERVICE, new GrpcService(mKeyValueServiceHandler));
    return services;
  }

  @Override
  public void start(WorkerNetAddress address) {
    // nothing to do, Thrift service will be started by the Alluxio worker process
  }

  @Override
  public void stop() throws IOException {
    // nothing to do, Thrift service will be started by the Alluxio worker process
  }
}
