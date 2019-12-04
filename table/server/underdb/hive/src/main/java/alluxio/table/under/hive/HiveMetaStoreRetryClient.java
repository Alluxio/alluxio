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

package alluxio.table.under.hive;

import alluxio.conf.ServerConfiguration;
import alluxio.resource.LockResource;
import alluxio.retry.CountingRetry;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static alluxio.conf.PropertyKey.TABLE_METASTORE_RETRY_TIMEOUT;

/**
 * A wrapper around HMS client that does caching and reconnection.
 */
public class HiveMetaStoreRetryClient implements MetaStoreClient {
  private HiveMetaStoreClient mHive = null;
  private final String mConnectionUri;
  private final String mHiveDbName;
  private final ReadWriteLock mLock;
  private final RetryPolicy mPolicy;

  /**
   * Constructor for the HiveMetaStoreRetryClient.
   * @param connectionUri connection uri to the metastore
   * @param hiveDbName the db name to access
   */
  public HiveMetaStoreRetryClient(String connectionUri, String hiveDbName) {
    mConnectionUri = connectionUri;
    mHiveDbName = hiveDbName;
    mLock = new ReentrantReadWriteLock();
    mPolicy = RetryUtils.defaultClientRetry(ServerConfiguration.getDuration(TABLE_METASTORE_RETRY_TIMEOUT),
        Duration.ofMillis(100), Duration.ofSeconds(5));
  }

  private HiveMetaStoreClient getHive() throws IOException {
    if (mHive != null) {
      return mHive;
    }
    newHiveClient();
    return mHive;
  }

  private void newHiveClient() throws IOException {
    HiveMetaStoreClient client;
    // Hive uses/saves the thread context class loader.
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try (LockResource w = new LockResource(mLock.writeLock())) {

      // use the extension class loader
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      HiveConf conf = new HiveConf();
      conf.set("hive.metastore.uris", mConnectionUri);
      client = new HiveMetaStoreClient(conf);
      client.getDatabase(mHiveDbName);
      mHive = client;
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
  public List<String> getAllTables(String dbname) throws MetaException, IOException {
    try {
      return RetryUtils.retry("getAllTables", () -> {
        try {
          return getHive().getAllTables(dbname);
        } catch (IOException | MetaException e) {
          mHive = null;
          throw new RetryUtils.RetryException("retry exception", e);
        }
      }, mPolicy);
    } catch (RetryUtils.CantRetryException e) {
      throw new IOException(e.getCause());
    }
  }

  @Override
  public Table getTable(String dbname, String name) throws MetaException, TException,
      NoSuchObjectException, IOException {
    try {
      return RetryUtils.retry("getTable", () -> {
        try {
          return getHive().getTable(dbname, name);
        } catch (NoSuchObjectException e) {
          throw new RetryUtils.CantRetryException("Cannot retry", e);
        } catch (IOException | TException e) {
          mHive = null;
          throw new RetryUtils.RetryException("Retry exception", e);
        }
      }, mPolicy);
    } catch (RetryUtils.CantRetryException e) {
      if (e.getCause() instanceof  NoSuchObjectException) {
        // unwrap the NoSuchObjectException if it is the cause for unable to retry
        throw (NoSuchObjectException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  @Override
  public List<Partition> listPartitions(String dbName, String tblName, short maxParts)
      throws NoSuchObjectException, MetaException, TException, IOException {
    try {
      return RetryUtils.retry("listPartitions", () -> {
        try {
          return getHive().listPartitions(dbName, tblName, maxParts);
        } catch (NoSuchObjectException e) {
          throw new RetryUtils.CantRetryException("Cannot retry", e);
        } catch (IOException | TException e) {
          mHive = null;
          throw new RetryUtils.RetryException("Retry exception", e);
        }
      }, mPolicy);
    } catch (RetryUtils.CantRetryException e) {
      if (e.getCause() instanceof  NoSuchObjectException) {
        // unwrap the NoSuchObjectException if it is the cause for unable to retry
        throw (NoSuchObjectException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws NoSuchObjectException, MetaException,
      TException, InvalidInputException, InvalidObjectException, IOException {
    try {
      return RetryUtils.retry("getTableColumnStatistics", () -> {
        try {
          return getHive().getTableColumnStatistics(dbName, tableName, colNames);
        } catch (NoSuchObjectException|InvalidInputException|InvalidObjectException e) {
          throw new RetryUtils.CantRetryException("Cannot retry", e);
        } catch (IOException | TException e) {
          mHive = null;
          throw new RetryUtils.RetryException("Retry exception", e);
        }
      }, mPolicy);
    } catch (RetryUtils.CantRetryException e) {
      if (e.getCause() instanceof  NoSuchObjectException) {
        // unwrap the NoSuchObjectException if it is the cause for unable to retry
        throw (NoSuchObjectException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName, String tableName, List<String> partNames, List<String> colNames)
      throws NoSuchObjectException, MetaException, TException, IOException {
    try {
      return RetryUtils.retry("getPartitionColumnStatistics", () -> {
        try {
          return getHive().getPartitionColumnStatistics(dbName, tableName, partNames, colNames);
        } catch (NoSuchObjectException|InvalidInputException|InvalidObjectException e) {
          throw new RetryUtils.CantRetryException("Cannot retry", e);
        } catch (IOException | TException e) {
          mHive = null;
          throw new RetryUtils.RetryException("Retry exception", e);
        }
      }, mPolicy);
    } catch (RetryUtils.CantRetryException e) {
      if (e.getCause() instanceof  NoSuchObjectException) {
        // unwrap the NoSuchObjectException if it is the cause for unable to retry
        throw (NoSuchObjectException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }
}
