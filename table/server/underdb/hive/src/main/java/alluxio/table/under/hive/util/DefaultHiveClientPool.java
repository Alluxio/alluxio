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

package alluxio.table.under.hive.util;

import static alluxio.conf.PropertyKey.TABLE_UDB_HIVE_CLIENTPOOL_MAX;
import static alluxio.conf.PropertyKey.TABLE_UDB_HIVE_CLIENTPOOL_MIN;

import alluxio.Constants;
import alluxio.conf.ServerConfiguration;
import alluxio.resource.CloseableResource;
import alluxio.util.ThreadFactoryUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A pool for hive clients, since hive clients are not thread safe.
 */
@ThreadSafe
public final class DefaultHiveClientPool extends AbstractHiveClientPool {
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(1, ThreadFactoryUtils.build("HiveClientPool-GC-%d", true));
  private static final HiveMetaHookLoader NOOP_HOOK = table -> null;

  private final long mGcThresholdMs;
  private final String mConnectionUri;

  /**
   * Creates a new hive client client pool.
   *
   * @param connectionUri the connect uri for the hive metastore
   */
  public DefaultHiveClientPool(String connectionUri) {
    super(Options.defaultOptions()
        .setMinCapacity(ServerConfiguration.getInt(TABLE_UDB_HIVE_CLIENTPOOL_MIN))
        .setMaxCapacity(ServerConfiguration.getInt(TABLE_UDB_HIVE_CLIENTPOOL_MAX))
        .setGcIntervalMs(5L * Constants.MINUTE_MS)
        .setGcExecutor(GC_EXECUTOR));
    mConnectionUri = connectionUri;
    mGcThresholdMs = 5L * Constants.MINUTE_MS;
  }

  @Override
  protected void closeResource(IMetaStoreClient client) {
    client.close();
  }

  @Override
  protected IMetaStoreClient createNewResource() throws IOException {
    // Hive uses/saves the thread context class loader.
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // use the extension class loader
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      HiveConf conf = new HiveConf();
      conf.verifyAndSet("hive.metastore.uris", mConnectionUri);

      IMetaStoreClient client = HMSClientFactory.newInstance(
          RetryingMetaStoreClient.getProxy(conf, NOOP_HOOK, HiveMetaStoreClient.class.getName()));
      return client;
    } catch (NullPointerException | TException e) {
      // HiveMetaStoreClient throws a NPE if the uri is not a uri for hive metastore
      throw new IOException(String
          .format("Failed to create client to hive metastore: %s. error: %s", mConnectionUri,
              e.getMessage()), e);
    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
  }

  @Override
  protected boolean isHealthy(IMetaStoreClient client) {
    // there is no way to check if a hive client is connected.
    // TODO(gpang): periodically and asynchronously check the health of clients
    return true;
  }

  @Override
  protected boolean shouldGc(ResourceInternal<IMetaStoreClient> clientResourceInternal) {
    return System.currentTimeMillis() - clientResourceInternal
        .getLastAccessTimeMs() > mGcThresholdMs;
  }

  @Override
  public CloseableResource<IMetaStoreClient> acquireClientResource() throws IOException {
    return new CloseableResource<IMetaStoreClient>(acquire()) {
      @Override
      public void close() {
        release(get());
      }
    };
  }
}
