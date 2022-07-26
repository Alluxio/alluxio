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

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcServerAddress;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.DynamicResourcePool;
import alluxio.util.ThreadFactoryUtils;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private final ClientContext mContext;
  private final GrpcServerAddress mAddress;
  private static final int WORKER_CLIENT_POOL_GC_THREADPOOL_SIZE = 10;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(WORKER_CLIENT_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("BlockWorkerClientPoolGcThreads-%d", true));
  private static final Counter COUNTER = MetricsSystem.counter(
      MetricKey.CLIENT_BLOCK_WORKER_CLIENT_COUNT.getName());
  private final AlluxioConfiguration mConf;

  /**
   * Create a block master client pool with default capacities.
   *
   * @param context Client context
   * @param address Address of worker
   * @param alluxioConf Alluxio Configuration
   */
  public BlockWorkerClientPool(ClientContext context,
                               GrpcServerAddress address, AlluxioConfiguration alluxioConf) {
    this(context, address,
            context.getClusterConf().getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_MIN),
            context.getClusterConf().getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_MAX),
            alluxioConf);
  }

  /**
   * Creates a new block master client pool.
   *
   * @param context the client context
   * @param address address of the worker
   * @param minCapacity the minimum capacity of the pool
   * @param maxCapacity the maximum capacity of the pool
   * @param alluxioConf Alluxio configuration
   */
  public BlockWorkerClientPool(ClientContext context, GrpcServerAddress address, int minCapacity,
                               int maxCapacity, AlluxioConfiguration alluxioConf) {
    super(Options.defaultOptions().setMinCapacity(minCapacity).setMaxCapacity(maxCapacity)
        .setGcExecutor(GC_EXECUTOR));
    mContext = Preconditions.checkNotNull(context);
    mAddress = Preconditions.checkNotNull(address);
    mConf = Preconditions.checkNotNull(alluxioConf);
  }

  @Override
  protected void closeResource(BlockWorkerClient client) throws IOException {
    LOG.debug("Block worker client for {} closed.", mAddress);
    client.close();
  }

  @Override
  protected BlockWorkerClient createNewResource() throws IOException {
    return BlockWorkerClient.Factory.create(mContext, mAddress, mConf);
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
    return System.currentTimeMillis() - clientResourceInternal.getLastAccessTimeMs()
        > mConf.getMs(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS);
  }
}
