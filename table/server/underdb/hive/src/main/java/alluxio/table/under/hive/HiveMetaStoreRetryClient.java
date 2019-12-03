package alluxio.table.under.hive;

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
import java.util.List;

/**
 * A wrapper around HMS client that does caching and reconnection.
 */
public class HiveMetaStoreRetryClient implements MetaStoreClient {
  private HiveMetaStoreClient mHive = null;
  private String mConnectionUri;
  private String mHiveDbName;

  /**
   * Constructor for the HiveMetaStoreRetryClient.
   * @param connectionUri connection uri to the metastore
   * @param hiveDbName the db name to access
   */
  public HiveMetaStoreRetryClient(String connectionUri, String hiveDbName) {
    mConnectionUri = connectionUri;
    mHiveDbName = hiveDbName;
  }

  private HiveMetaStoreClient getHive() throws IOException {
    if (mHive != null) {
      return mHive;
    }

    mHive = newHiveClient();
    return mHive;
  }

  private HiveMetaStoreClient newHiveClient() throws IOException {
    HiveMetaStoreClient client;
    // Hive uses/saves the thread context class loader.
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // use the extension class loader
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      HiveConf conf = new HiveConf();
      conf.set("hive.metastore.uris", mConnectionUri);
      client = new HiveMetaStoreClient(conf);
      client.getDatabase(mHiveDbName);
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
  public List<String> getAllTables(String dbname) throws MetaException, IOException {
    try {
      return getHive().getAllTables(dbname);
    } catch (TException e) {
      mHive = newHiveClient();
      return getHive().getAllTables(dbname);
    }
  }

  @Override
  public Table getTable(String dbname, String name) throws MetaException, TException,
      NoSuchObjectException, IOException {
    try {
      return getHive().getTable(dbname, name);
    } catch (TException e) {
      mHive = newHiveClient();
      return getHive().getTable(dbname, name);
    }
  }

  @Override
  public List<Partition> listPartitions(String dbName, String tblName, short maxParts)
      throws NoSuchObjectException, MetaException, TException, IOException {
    try {
      return getHive().listPartitions(dbName, tblName, maxParts);
    } catch (TException e) {
      mHive = newHiveClient();
      return getHive().listPartitions(dbName, tblName, maxParts);
    }
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws NoSuchObjectException, MetaException,
      TException, InvalidInputException, InvalidObjectException, IOException {
    try {
      return getHive().getTableColumnStatistics(dbName, tableName, colNames);
    } catch (TException e) {
      mHive = newHiveClient();
      return getHive().getTableColumnStatistics(dbName, tableName, colNames);
    }
  }
}
