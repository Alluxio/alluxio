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

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.FileStatistics;
import alluxio.table.common.UdbContext;
import alluxio.table.common.UdbTable;
import alluxio.table.common.UnderDatabase;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.URIUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Hive database implementation.
 */
public class HiveDatabase implements UnderDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(HiveDatabase.class);
  private static final String DEFAULT_DB_NAME = "default";

  private final UdbContext mUdbContext;
  private final HiveDataCatalog mCatalog;
  private final Hive mHive;

  private HiveDatabase(UdbContext udbContext, HiveDataCatalog catalog, Hive hive) {
    mUdbContext = udbContext;
    mCatalog = catalog;
    mHive = hive;
  }

  /**
   * Creates an instance of the Hive database UDB.
   *
   * @param udbContext the db context
   * @param configuration the configuration
   * @return the new instance
   */
  public static HiveDatabase create(UdbContext udbContext, Map<String, String> configuration)
      throws IOException {
    UnderFileSystem ufs;
    if (URIUtils.isLocalFilesystem(ServerConfiguration
        .get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS))) {
      ufs = UnderFileSystem.Factory
          .create("/", UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
    } else {
      ufs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
    }
    HiveDataCatalog catalog = new HiveDataCatalog(ufs);
    // TODO(gpang): get rid of creating db
    catalog.createDatabase(DEFAULT_DB_NAME);

    Hive hive;
    try {
      HiveConf conf = new HiveConf();
      // TODO(gpang): use configuration keys passed in
      conf.set("hive.metastore.uris", "thrift://localhost:9083");
      hive = Hive.get(conf);
    } catch (HiveException e) {
      throw new IOException("Failed to create hive client: " + e.getMessage(), e);
    }
    return new HiveDatabase(udbContext, catalog, hive);
  }

  @Override
  public String getType() {
    return HiveDatabaseFactory.TYPE;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    try {
      return mHive.getAllTables(DEFAULT_DB_NAME);
    } catch (HiveException e) {
      throw new IOException("Failed to get hive tables: " + e.getMessage(), e);
    }
  }

  @Override
  public UdbTable getTable(String tableName) throws IOException {
    Table table = null;
    try {
      table = mHive.getTable(DEFAULT_DB_NAME, tableName);
      AlluxioURI tableUri = mUdbContext.getTableLocation(tableName);
      // make sure the parent exists
      mUdbContext.getFileSystem().createDirectory(tableUri.getParent(),
          CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
//      mDbContext.getFileSystem()
//          .mount(tableUri, new AlluxioURI(table.getDataLocation().toString()));
      mUdbContext.getFileSystem().mount(tableUri, new AlluxioURI("/tmp/speedcore/table"));
      LOG.info("mounted table {} location {} to Alluxio location {}", tableName,
          table.getDataLocation(), tableUri);
      // TODO(gpang): manage the mount mapping for statistics/metadata
      return new HiveTable(tableName, HiveUtils.toProto(table.getAllCols()), tableUri.getPath(),
          null);
    } catch (HiveException e) {
      throw new IOException("Failed to get table: " + tableName + " error: " + e.getMessage(), e);
    } catch (AlluxioException e) {
      throw new IOException(
          "Failed to mount table location. tableName: " + tableName
              + " tableLocation: " + (table != null ? table.getDataLocation() : "null")
              + " AlluxioLocation: " + mUdbContext.getTableLocation(tableName)
              + " error: " + e.getMessage(), e);
    }
  }

  @Override
  public Map<String, FileStatistics> getStatistics(String dbName, String tableName)
      throws IOException {
    mCatalog.getTable(TableIdentifier.of(DEFAULT_DB_NAME, tableName));
    return null;
  }
}
