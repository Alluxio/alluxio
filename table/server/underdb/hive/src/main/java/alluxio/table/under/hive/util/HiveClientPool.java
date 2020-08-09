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

import alluxio.Constants;
import alluxio.resource.CloseableResource;
import alluxio.resource.DynamicResourcePool;
import alluxio.util.ThreadFactoryUtils;

import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A pool for hive clients, since hive clients are not thread safe.
 */
@ThreadSafe
public final class HiveClientPool extends DynamicResourcePool<IMetaStoreClient> {
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(1, ThreadFactoryUtils.build("HiveClientPool-GC-%d", true));

  private final CloseableMetaStoreClientFactory mClientFactory =
      new CloseableMetaStoreClientFactory();

  private final long mGcThresholdMs;
  private final String mConnectionUri;
  private final String mHiveDbName;
  /** This tracks if the db exists in HMS. */
  private volatile boolean mDbExists = false;

  /**
   * Creates a new hive client client pool.
   *
   * @param connectionUri the connect uri for the hive metastore
   * @param hiveDbName the db name in hive
   */
  public HiveClientPool(String connectionUri, String hiveDbName) {
    super(Options.defaultOptions()
        .setMinCapacity(16)
        .setMaxCapacity(128)
        .setGcIntervalMs(5L * Constants.MINUTE_MS)
        .setGcExecutor(GC_EXECUTOR));
    mConnectionUri = connectionUri;
    mHiveDbName = hiveDbName;
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

      IMetaStoreClient client = mClientFactory.newInstance(conf, "hms");
      if (!mDbExists) {
        synchronized (this) {
          // serialize the querying of the hive db
          if (!mDbExists) {
            client.getDatabase(mHiveDbName);
            mDbExists = true;
          }
        }
      }
      return client;
    } catch (NoSuchObjectException e) {
      throw new IOException(String
          .format("hive db name '%s' does not exist at metastore: %s", mHiveDbName, mConnectionUri),
          e);
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

  /**
   * @return a closeable resource for the hive client
   */
  public CloseableResource<IMetaStoreClient> acquireClientResource() throws IOException {
    return new CloseableResource<IMetaStoreClient>(acquire()) {
      @Override
      public void close() {
        release(get());
      }
    };
  }
}
