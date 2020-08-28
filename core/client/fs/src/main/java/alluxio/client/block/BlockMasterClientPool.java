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

package alluxio.client.block;

import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.resource.DynamicResourcePool;
import alluxio.resource.ResourcePool;
import alluxio.util.ThreadFactoryUtils;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for managing block master clients. After obtaining a client with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
public final class BlockMasterClientPool extends DynamicResourcePool<BlockMasterClient> {
  private final MasterClientContext mMasterContext;
  private final long mGcThresholdMs;

  private static final int BLOCK_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE = 1;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(BLOCK_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("BlockMasterClientPoolGcThreads-%d", true));

  /**
   * Creates a new block master client pool.
   *
   * @param ctx the information required for connecting to Alluxio
   */
  public BlockMasterClientPool(MasterClientContext ctx) {
    super(Options.defaultOptions()
        .setMinCapacity(ctx.getClusterConf()
            .getInt(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MIN))
        .setMaxCapacity(ctx.getClusterConf()
            .getInt(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX))
        .setGcIntervalMs(
            ctx.getClusterConf().getMs(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_GC_INTERVAL_MS))
        .setGcExecutor(GC_EXECUTOR));
    mMasterContext = ctx;
    mGcThresholdMs =
        ctx.getClusterConf().getMs(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_GC_THRESHOLD_MS);
  }

  @Override
  protected void closeResource(BlockMasterClient client) {
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected BlockMasterClient createNewResource() {
    return BlockMasterClient.Factory.create(mMasterContext);
  }

  @Override
  protected boolean isHealthy(BlockMasterClient client) {
    return client.isConnected();
  }

  @Override
  protected boolean shouldGc(ResourceInternal<BlockMasterClient> clientResourceInternal) {
    return System.currentTimeMillis()
        - clientResourceInternal.getLastAccessTimeMs() > mGcThresholdMs;
  }
}
