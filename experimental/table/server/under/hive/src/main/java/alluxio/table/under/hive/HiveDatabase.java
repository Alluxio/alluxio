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
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.ColumnStatistics;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.FileStatistics;
import alluxio.grpc.MountPOptions;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.under.hive.util.PathTranslator;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.URIUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hive database implementation.
 */
public class HiveDatabase implements UnderDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(HiveDatabase.class);

  private final UdbContext mUdbContext;
  private final UdbConfiguration mConfiguration;
  private final HiveDataCatalog mCatalog;
  private final Hive mHive;
  private final String mDbName;

  private HiveDatabase(UdbContext udbContext, UdbConfiguration configuration,
      HiveDataCatalog catalog, Hive hive, String dbName) {
    mUdbContext = udbContext;
    mConfiguration = configuration;
    mCatalog = catalog;
    mHive = hive;
    mDbName = dbName;
    mConfiguration.toString(); // read the field
  }

  /**
   * Creates an instance of the Hive database UDB.
   *
   * @param udbContext the db context
   * @param configuration the configuration
   * @return the new instance
   */
  public static HiveDatabase create(UdbContext udbContext, UdbConfiguration configuration)
      throws IOException {
    String uris = configuration.get(Property.HIVE_METASTORE_URIS);
    if (uris.isEmpty()) {
      throw new IOException("Hive metastore uris is not configured. Please set parameter: "
          + Property.HIVE_METASTORE_URIS.getFullName(HiveDatabaseFactory.TYPE));
    }
    String dbName = configuration.get(Property.DATABASE_NAME);
    if (dbName.isEmpty()) {
      throw new IOException("Hive database name is not configured. Please set parameter: "
          + Property.DATABASE_NAME.getFullName(HiveDatabaseFactory.TYPE));
    }

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
    catalog.createDatabase(dbName);

    Hive hive;
    try {
      HiveConf conf = new HiveConf();
      conf.set("hive.metastore.uris", uris);
      hive = Hive.get(conf);
    } catch (HiveException e) {
      throw new IOException("Failed to create hive client: " + e.getMessage(), e);
    }
    return new HiveDatabase(udbContext, configuration, catalog, hive, dbName);
  }

  /**
   * @return the udb context
   */
  public UdbContext getUdbContext() {
    return mUdbContext;
  }

  @Override
  public String getType() {
    return HiveDatabaseFactory.TYPE;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    try {
      return mHive.getAllTables(mDbName);
    } catch (HiveException e) {
      throw new IOException("Failed to get hive tables: " + e.getMessage(), e);
    }
  }

  @Override
  public UdbTable getTable(String tableName) throws IOException {
    Table table = null;
    try {
      table = mHive.getTable(mDbName, tableName);

      AlluxioURI tableUri = mUdbContext.getTableLocation(tableName);

      String mountOptions = mConfiguration.get(Property.MOUNT_OPTIONS);
      Map<String, String> mountOptionMap = Collections.emptyMap();
      if (!mountOptions.isEmpty()) {
        mountOptionMap = new ObjectMapper().readValue(
            mountOptions, new TypeReference<Map<String, String>>() {});
      }

      // make sure the parent exists
      mUdbContext.getFileSystem().createDirectory(tableUri.getParent(),
          CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
      AlluxioURI ufsLocation = new AlluxioURI(table.getDataLocation().toString());

      String mountOption = mountOptionMap.get(ufsLocation.getAuthority().toString());
      MountPOptions option = MountPOptions.getDefaultInstance();
      if (mountOption != null) {
        // it has a mount option to be passed through
        JsonFormat.Parser jsonParser = JsonFormat.parser();
        MountPOptions.Builder builder = MountPOptions.newBuilder();
        jsonParser.merge(mountOption, builder);
        option = builder.build();
      }

      mUdbContext.getFileSystem().mount(tableUri, ufsLocation, option);
      LOG.info("mounted table {} location {} to Alluxio location {} with mountOption {}",
          tableName, table.getDataLocation(), tableUri, mountOption);
      PathTranslator pathTranslator =
          new PathTranslator();
      pathTranslator.addMapping(tableUri.toString(), table.getDataLocation().toString());

      List<String> colNames = table.getAllCols().stream().map(FieldSchema::getName)
          .collect(Collectors.toList());
      List<ColumnStatisticsObj> columnStats =
          mHive.getTableColumnStatistics(mDbName, tableName, colNames);
      FileStatistics.Builder builder = FileStatistics.newBuilder();
      for (ColumnStatisticsObj columnStat : columnStats) {
        if (columnStat.isSetStatsData()) {
          long distinctCount;
          switch (columnStat.getColType()) {
            case "string":
              distinctCount = columnStat.getStatsData().getStringStats().getNumDVs();
              break;
            case "int":
              distinctCount = columnStat.getStatsData().getDecimalStats().getNumDVs();
              break;
            case "float":
              distinctCount = columnStat.getStatsData().getDoubleStats().getNumDVs();
              break;
            case "bigint":
              distinctCount = columnStat.getStatsData().getLongStats().getNumDVs();
              break;
            default:
              distinctCount = -1;
          }
          builder.putColumn(columnStat.getColName(),
              ColumnStatistics.newBuilder().setRecordCount(distinctCount).build());
        }
      }
      // Potentially expensive call
      List<Partition> partitions = mHive.getPartitions(table);
      return new HiveTable(this, pathTranslator, tableName,
          HiveUtils.toProtoSchema(table.getAllCols()), tableUri.getPath(),
          Collections.singletonMap("unpartitioned", builder.build()),
          HiveUtils.toProto(table.getPartitionKeys()), partitions, table);
    } catch (InvalidTableException e) {
      throw new NotFoundException("Table " + tableName + " does not exist.", e);
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
    mCatalog.getTable(TableIdentifier.of(mDbName, tableName));
    return null;
  }
}
