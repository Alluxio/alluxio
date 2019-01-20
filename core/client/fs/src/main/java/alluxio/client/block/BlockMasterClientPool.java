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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient;
import alluxio.resource.DynamicResourcePool;
import alluxio.resource.ResourcePool;
import alluxio.util.ThreadFactoryUtils;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * Class for managing block master clients. After obtaining a client with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
public final class BlockMasterClientPool extends DynamicResourcePool<BlockMasterClient> {
  private final long mGcThresholdMs;
  private final MasterInquireClient mMasterInquireClient;
  private final Subject mSubject;

  private static final int BLOCK_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE = 1;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(BLOCK_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("BlockMasterClientPoolGcThreads-%d", true));

  /**
   * Creates a new block master client pool.
   *
   * @param subject the parent subject
   * @param masterInquireClient a client for determining the master address
   */
  public BlockMasterClientPool(Subject subject, MasterInquireClient masterInquireClient) {
    super(Options.defaultOptions()
        .setMinCapacity(Configuration.getInt(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MIN))
        .setMaxCapacity(Configuration.getInt(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX))
        .setGcIntervalMs(
            Configuration.getMs(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_GC_INTERVAL_MS))
        .setGcExecutor(GC_EXECUTOR));
    mGcThresholdMs =
        Configuration.getMs(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_GC_THRESHOLD_MS);
    mSubject = subject;
    mMasterInquireClient = masterInquireClient;
  }

  @Override
  protected void closeResource(BlockMasterClient client) {
    closeResourceSync(client);
  }

  @Override
  public void closeResourceSync(BlockMasterClient client) {
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected BlockMasterClient createNewResource() {
    BlockMasterClient client = BlockMasterClient.Factory.create(MasterClientConfig.defaults()
        .withSubject(mSubject).withMasterInquireClient(mMasterInquireClient));
    return client;
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
