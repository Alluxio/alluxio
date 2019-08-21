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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.experimental.ProtoUtils;
import alluxio.grpc.FileStatistics;
import alluxio.table.common.UdbTable;
import alluxio.table.common.UnderDatabase;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.URIUtils;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;
import java.util.Map;

/**
 * Hive database implementation.
 */
public class HiveDatabase implements UnderDatabase {
  private static final String DEFAULT_DB_NAME = "default";

  private final HiveDataCatalog mCatalog;
  private final UnderFileSystem mUfs;

  /**
   * Creates an instance of the Hive database UDB.
   */
  public HiveDatabase() {
    if (URIUtils.isLocalFilesystem(ServerConfiguration
        .get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS))) {
      mUfs = UnderFileSystem.Factory
          .create("/", UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
    } else {
      mUfs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
    }
    mCatalog = new HiveDataCatalog(mUfs);
    // TODO(gpang): get rid of creating db
    mCatalog.createDatabase(DEFAULT_DB_NAME);
  }

  @Override
  public String getType() {
    return HiveDatabaseFactory.TYPE;
  }

  @Override
  public List<String> getTableNames() {
    return mCatalog.getAllTables(DEFAULT_DB_NAME);
  }

  @Override
  public UdbTable getTable(String tableName) {
    Table table = mCatalog.getTable(TableIdentifier.of(DEFAULT_DB_NAME, tableName));
    return new HiveTable(tableName, ProtoUtils.toProto(table.schema()), table.location(), null);
  }

  @Override
  public Map<String, FileStatistics> getStatistics(String dbName, String tableName) {
    return null;
  }
}
