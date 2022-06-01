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

package alluxio.client.block.stream;

import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcServerAddress;
import alluxio.master.MasterClientContext;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.DynamicResourcePool;
import alluxio.security.user.UserState;
import alluxio.util.ThreadFactoryUtils;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for managing block worker clients. After obtaining a client with
 * {@link DynamicResourcePool#acquire()}, {@link DynamicResourcePool#release(Object)} must be called
 * when the thread is done using the client.
 */
@ThreadSafe
public final class BlockWorkerClientPool extends DynamicResourcePool<BlockWorkerClient> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerClientPool.class);
  private final UserState mUserState;
  private final GrpcServerAddress mAddress;
  private static final int WORKER_CLIENT_POOL_GC_THREADPOOL_SIZE = 10;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(WORKER_CLIENT_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("BlockWorkerClientPoolGcThreads-%d", true));
  private static final Counter COUNTER = MetricsSystem.counter(
      MetricKey.CLIENT_BLOCK_WORKER_CLIENT_COUNT.getName());
  private final MasterClientContext mMasterContext;
  private final long mGcThresholdMs;

  /**
   * Creates a new block master client pool.
   *
   * @param userState the parent userState
   * @param address address of the worker
   * @param ctx the information required for connecting to Alluxio
   */
  public BlockWorkerClientPool(UserState userState, GrpcServerAddress address,
      MasterClientContext ctx) {
    super(Options.defaultOptions()
        .setMinCapacity(ctx.getClusterConf()
            .getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_MIN))
        .setMaxCapacity(ctx.getClusterConf()
            .getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_MAX))
        .setGcIntervalMs(
            ctx.getClusterConf().getMs(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_GC_INTERVAL_MS))
        .setGcExecutor(GC_EXECUTOR));
    Objects.requireNonNull(userState);
    mUserState = userState;
    Objects.requireNonNull(address);
    mAddress = address;
    mMasterContext = ctx;
    mGcThresholdMs =
        ctx.getClusterConf().getMs(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS);
  }

  @Override
  protected void closeResource(BlockWorkerClient client) throws IOException {
    LOG.debug("Block worker client for {} closed.", mAddress);
    client.close();
  }

  @Override
  protected BlockWorkerClient createNewResource() throws IOException {
    return BlockWorkerClient.Factory.create(mUserState, mAddress, mMasterContext.getClusterConf());
  }

  /**
   * Checks whether a client is healthy.
   *
   * @param client the client to check
   * @return true if the client is active (i.e. connected)
   */
  @Override
  protected boolean isHealthy(BlockWorkerClient client) {
    return client.isHealthy();
  }

  @Override
  protected Counter getMetricCounter() {
    return COUNTER;
  }

  @Override
  protected boolean shouldGc(ResourceInternal<BlockWorkerClient> clientResourceInternal) {
    return System.currentTimeMillis() - clientResourceInternal
        .getLastAccessTimeMs() > mGcThresholdMs;
  }
}
