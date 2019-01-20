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

<<<<<<< HEAD
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
=======
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient;
import alluxio.resource.DynamicResourcePool;
>>>>>>> 082ccd3594... [ALLUXIO-3394] GC fs master client (#8263)
import alluxio.resource.ResourcePool;
import alluxio.util.ThreadFactoryUtils;

import sun.misc.GC;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
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
<<<<<<< HEAD
public final class BlockMasterClientPool extends ResourcePool<BlockMasterClient> {
  private final MasterClientContext mMasterContext;
  private final Queue<BlockMasterClient> mClientList;
=======
public final class BlockMasterClientPool extends DynamicResourcePool<BlockMasterClient> {
  private final long mGcThresholdMs;
  private final MasterInquireClient mMasterInquireClient;
  private final Subject mSubject;
>>>>>>> 082ccd3594... [ALLUXIO-3394] GC fs master client (#8263)

  private static final int BLOCK_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE = 1;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(BLOCK_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("BlockMasterClientPoolGcThreads-%d", true));

  /**
   * Creates a new block master client pool.
   *
   * @param ctx the information required for connecting to Alluxio
   */
<<<<<<< HEAD
  public BlockMasterClientPool(MasterClientContext ctx) {
    super(ctx.getClusterConf().getInt(PropertyKey.USER_BLOCK_MASTER_CLIENT_THREADS));
    mClientList = new ConcurrentLinkedQueue<>();
    mMasterContext = ctx;
=======
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
>>>>>>> 082ccd3594... [ALLUXIO-3394] GC fs master client (#8263)
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
<<<<<<< HEAD
    BlockMasterClient client = BlockMasterClient.Factory.create(mMasterContext);
    mClientList.add(client);
=======
    BlockMasterClient client = BlockMasterClient.Factory.create(MasterClientConfig.defaults()
        .withSubject(mSubject).withMasterInquireClient(mMasterInquireClient));
>>>>>>> 082ccd3594... [ALLUXIO-3394] GC fs master client (#8263)
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
