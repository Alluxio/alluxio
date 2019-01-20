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

package alluxio.client.file;

<<<<<<< HEAD
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.resource.ResourcePool;

import com.google.common.io.Closer;
=======
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient;
import alluxio.resource.DynamicResourcePool;
import alluxio.util.ThreadFactoryUtils;
>>>>>>> 082ccd3594... [ALLUXIO-3394] GC fs master client (#8263)

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A fixed pool of FileSystemMasterClient instances.
 */
@ThreadSafe
<<<<<<< HEAD
public final class FileSystemMasterClientPool extends ResourcePool<FileSystemMasterClient> {
  private final Queue<FileSystemMasterClient> mClientList;
  private final MasterClientContext mMasterContext;
=======
public final class FileSystemMasterClientPool extends DynamicResourcePool<FileSystemMasterClient> {
  private final long mGcThresholdMs;
  private final MasterInquireClient mMasterInquireClient;
  private final Subject mSubject;
>>>>>>> 082ccd3594... [ALLUXIO-3394] GC fs master client (#8263)

  private static final int FS_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE = 1;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(FS_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("FileSystemMasterClientPoolGcThreads-%d", true));

  /**
   * Creates a new file system master client pool.
   *
   * @param ctx information for connecting to processes in the cluster
   */
<<<<<<< HEAD
  public FileSystemMasterClientPool(MasterClientContext ctx) {
    super(ctx.getClusterConf().getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS));
    mClientList = new ConcurrentLinkedQueue<>();
    mMasterContext = ctx;
=======
  public FileSystemMasterClientPool(Subject subject, MasterInquireClient masterInquireClient) {
    super(Options.defaultOptions()
        .setMinCapacity(Configuration.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MIN))
        .setMaxCapacity(Configuration.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX))
        .setGcIntervalMs(
            Configuration.getMs(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_GC_INTERVAL_MS))
        .setGcExecutor(GC_EXECUTOR));
    mGcThresholdMs =
        Configuration.getMs(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_GC_THRESHOLD_MS);
    mMasterInquireClient = masterInquireClient;
    mSubject = subject;
  }

  /**
   * Creates a new file system master client pool.
   *
   * @param subject the parent subject
   * @param masterInquireClient a client for determining the master address
   * @param clientThreads the number of client threads to use
   */
  public FileSystemMasterClientPool(Subject subject, MasterInquireClient masterInquireClient,
      int clientThreads) {
    super(Options.defaultOptions()
        .setMinCapacity(Configuration.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MIN))
        .setMaxCapacity(clientThreads)
        .setGcExecutor(GC_EXECUTOR));
    mGcThresholdMs =
        Configuration.getMs(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_GC_THRESHOLD_MS);
    mMasterInquireClient = masterInquireClient;
    mSubject = subject;
>>>>>>> 082ccd3594... [ALLUXIO-3394] GC fs master client (#8263)
  }

  @Override
  protected void closeResource(FileSystemMasterClient client) {
    closeResourceSync(client);
  }

  @Override
  protected void closeResourceSync(FileSystemMasterClient client) {
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected FileSystemMasterClient createNewResource() {
<<<<<<< HEAD
    FileSystemMasterClient client = FileSystemMasterClient.Factory.create(mMasterContext);
    mClientList.add(client);
=======
    FileSystemMasterClient client = FileSystemMasterClient.Factory.create(MasterClientConfig
        .defaults().withSubject(mSubject).withMasterInquireClient(mMasterInquireClient));
>>>>>>> 082ccd3594... [ALLUXIO-3394] GC fs master client (#8263)
    return client;
  }

  @Override
  protected boolean isHealthy(FileSystemMasterClient client) {
    return client.isConnected();
  }

  @Override
  protected boolean shouldGc(ResourceInternal<FileSystemMasterClient> clientResourceInternal) {
    return System.currentTimeMillis() - clientResourceInternal
        .getLastAccessTimeMs() > mGcThresholdMs;
  }
}
