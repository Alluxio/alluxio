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
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.FileStatistics;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;
import alluxio.table.under.hive.util.PathTranslator;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.URIUtils;
import alluxio.util.io.PathUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private final HiveMetaStoreClient mHive;
  /** the name of the hive db. */
  private final String mHiveDbName;

  private HiveDatabase(UdbContext udbContext, UdbConfiguration configuration,
      HiveDataCatalog catalog, HiveMetaStoreClient hive, String hiveDbName) {
    mUdbContext = udbContext;
    mConfiguration = configuration;
    mCatalog = catalog;
    mHive = hive;
    mHiveDbName = hiveDbName;
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

    HiveMetaStoreClient hive;
    try {
      HiveConf conf = new HiveConf();
      conf.set("hive.metastore.uris", uris);
      hive = new HiveMetaStoreClient(conf);
    } catch (MetaException e) {
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
  public String getName() {
    return mHiveDbName;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    try {
      return mHive.getAllTables(mHiveDbName);
    } catch (MetaException e) {
      throw new IOException("Failed to get hive tables: " + e.getMessage(), e);
    }
  }

  private String mountAlluxioPath(String tableName, AlluxioURI ufsUri, AlluxioURI tableUri,
      PathTranslator translator) throws IOException, AlluxioException {
    try {
      tableUri = mUdbContext.getFileSystem().reverseResolve(ufsUri);
      translator.addMapping(tableUri, ufsUri);
      LOG.info("Trying to mount table {} location {}, but table {} already mounted at location {}",
          tableName, ufsUri, tableUri);
      return tableUri.getPath();
    } catch (InvalidPathException e) {
      // ufs path not mounted, continue
    }
    // make sure the parent exists
    mUdbContext.getFileSystem().createDirectory(tableUri.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
    Map<String, String> mountOptionMap = mConfiguration.getMountOption(
        ufsUri.getScheme() + "://" + ufsUri.getAuthority().toString());
    MountPOptions.Builder option = MountPOptions.newBuilder();
    for (Map.Entry<String, String> entry : mountOptionMap.entrySet()) {
      if (entry.getKey().equals(UdbConfiguration.READ_ONLY_OPTION)) {
        option.setReadOnly(Boolean.parseBoolean(entry.getValue()));
      } else if (entry.getKey().equals(UdbConfiguration.SHARED_OPTION)) {
        option.setShared(Boolean.parseBoolean(entry.getValue()));
      } else {
        option.putProperties(entry.getKey(), entry.getValue());
      }
    }
    mUdbContext.getFileSystem().mount(tableUri, ufsUri, option.build());
    translator.addMapping(tableUri, ufsUri);

    LOG.info("mounted table {} location {} to Alluxio location {} with mountOption {}",
        tableName, ufsUri, tableUri, option.build());
    return tableUri.getPath();
  }

  private PathTranslator mountAlluxioPaths(Table table, List<Partition> partitions)
      throws IOException {
    String tableName = table.getTableName();
    AlluxioURI ufsUri = null;
    AlluxioURI alluxioUri = mUdbContext.getTableLocation(tableName);

    try {
      PathTranslator pathTranslator =
          new PathTranslator();
      ufsUri = new AlluxioURI(table.getSd().getLocation());
      mountAlluxioPath(tableName, ufsUri, alluxioUri, pathTranslator);

      for (Partition part : partitions) {
        AlluxioURI partitionUri;
        if (part.getSd() != null && part.getSd().getLocation() != null
            && ufsUri.isAncestorOf(partitionUri = new AlluxioURI(part.getSd().getLocation()))) {
          String partName = part.getValues().toString();
          try {
            partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
          } catch (MetaException e) {
            LOG.warn("Error making partition name for table {}, partition {}", tableName,
                part.getValues().toString());
          }
          alluxioUri = new AlluxioURI(PathUtils.concatPath(
              mUdbContext.getTableLocation(tableName).getPath(), partName));

          // mount partition path if it is not already mounted as part of the table path mount
          mountAlluxioPath(tableName, partitionUri, alluxioUri, pathTranslator);
        }
      }
      return pathTranslator;
    } catch (AlluxioException e) {
      throw new IOException(
          "Failed to mount table location. tableName: " + tableName
              + " ufsUri: " + ((ufsUri == null) ? "" : ufsUri.getPath())
              + " AlluxioLocation: " + alluxioUri
              + " error: " + e.getMessage(), e);
    }
  }

  @Override
  public UdbTable getTable(String tableName) throws IOException {
    Table table;
    try {
      table = mHive.getTable(mHiveDbName, tableName);

      // Potentially expensive call
      List<Partition> partitions =
          mHive.listPartitions(mHiveDbName, table.getTableName(), (short) -1);

      PathTranslator pathTranslator = mountAlluxioPaths(table, partitions);
      List<String> colNames = table.getSd().getCols().stream().map(FieldSchema::getName)
          .collect(Collectors.toList());
      List<ColumnStatisticsObj> columnStats =
          mHive.getTableColumnStatistics(mHiveDbName, tableName, colNames);

      List<ColumnStatisticsInfo> colStats =
          columnStats.stream().map(HiveUtils::toProto).collect(Collectors.toList());

      return new HiveTable(mHive, this, pathTranslator, tableName,
          HiveUtils.toProtoSchema(table.getSd().getCols()),
          pathTranslator.toAlluxioPath(new AlluxioURI(table.getSd().getLocation())).toString(),
          colStats,
          HiveUtils.toProto(table.getPartitionKeys()), partitions, table);
    } catch (NoSuchObjectException e) {
      throw new NotFoundException("Table " + tableName + " does not exist.", e);
    } catch (TException e) {
      throw new IOException("Failed to get table: " + tableName + " error: " + e.getMessage(), e);
    }
  }

  @Override
  public Map<String, FileStatistics> getStatistics(String dbName, String tableName)
      throws IOException {
    mCatalog.getTable(TableIdentifier.of(mHiveDbName, tableName));
    return null;
  }
}
