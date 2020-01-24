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
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.master.table.DatabaseInfo;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;
import alluxio.table.under.hive.util.PathTranslator;
import alluxio.util.io.PathUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Hive database implementation.
 */
public class HiveDatabase implements UnderDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(HiveDatabase.class);
  private static final HiveMetaHookLoader NOOP_HOOK = table -> null;

  private final UdbContext mUdbContext;
  private final UdbConfiguration mConfiguration;
  /** the connection uri for the hive metastore. */
  private final String mConnectionUri;
  /** the name of the hive db. */
  private final String mHiveDbName;

  /** The cached hive client. This should not be used directly, but via {@link #getHive()}. */
  private IMetaStoreClient mHive = null;

  private HiveDatabase(UdbContext udbContext, UdbConfiguration configuration,
      String connectionUri, String hiveDbName) {
    mUdbContext = udbContext;
    mConfiguration = configuration;
    mConnectionUri = connectionUri;
    mHiveDbName = hiveDbName;
  }

  /**
   * Creates an instance of the Hive database UDB.
   *
   * @param udbContext the db context
   * @param configuration the configuration
   * @return the new instance
   */
  public static HiveDatabase create(UdbContext udbContext, UdbConfiguration configuration) {
    String connectionUri = udbContext.getConnectionUri();
    if (connectionUri == null || connectionUri.isEmpty()) {
      throw new IllegalArgumentException(
          "Hive udb connection uri cannot be empty: " + connectionUri);
    }
    String hiveDbName = udbContext.getUdbDbName();
    if (hiveDbName == null || hiveDbName.isEmpty()) {
      throw new IllegalArgumentException("Hive database name cannot be empty: " + hiveDbName);
    }

    return new HiveDatabase(udbContext, configuration, connectionUri, hiveDbName);
  }

  @Override
  public UdbContext getUdbContext() {
    return mUdbContext;
  }

  @Override
  public DatabaseInfo getDatabaseInfo() throws IOException {
    try {
      Database hiveDb = getHive().getDatabase(mHiveDbName);
      alluxio.grpc.table.PrincipalType type = alluxio.grpc.table.PrincipalType.USER;
      if (hiveDb.getOwnerType().equals(PrincipalType.ROLE)) {
        type = alluxio.grpc.table.PrincipalType.ROLE;
      }
      return new DatabaseInfo(hiveDb.getLocationUri(), hiveDb.getOwnerName(), type,
          hiveDb.getDescription(), hiveDb.getParameters());
    } catch (TException  e) {
      throw new IOException("Failed to get hive database " + mHiveDbName
          + ". " + e.getMessage(), e);
    }
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
      return getHive().getAllTables(mHiveDbName);
    } catch (TException  e) {
      throw new IOException("Failed to get hive tables: " + e.getMessage(), e);
    }
  }

  private String mountAlluxioPath(String tableName, AlluxioURI ufsUri, AlluxioURI tableUri)
      throws IOException, AlluxioException {
    if (Objects.equals(ufsUri.getScheme(), Constants.SCHEME)) {
      // already an alluxio uri, return the alluxio uri
      return ufsUri.toString();
    }
    try {
      tableUri = mUdbContext.getFileSystem().reverseResolve(ufsUri);
      LOG.debug("Trying to mount table {} location {}, but it is already mounted at location {}",
          tableName, ufsUri, tableUri);
      return tableUri.getPath();
    } catch (InvalidPathException e) {
      // ufs path not mounted, continue
    }
    // make sure the parent exists
    mUdbContext.getFileSystem().createDirectory(tableUri.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
    Map<String, String> mountOptionMap = mConfiguration.getMountOption(
        String.format("%s://%s/", ufsUri.getScheme(), ufsUri.getAuthority().toString()));
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

    LOG.info("mounted table {} location {} to Alluxio location {} with mountOption {}",
        tableName, ufsUri, tableUri, option.build());
    return tableUri.getPath();
  }

  private PathTranslator mountAlluxioPaths(Table table, List<Partition> partitions)
      throws IOException {
    String tableName = table.getTableName();
    AlluxioURI ufsUri;
    AlluxioURI alluxioUri = mUdbContext.getTableLocation(tableName);
    String hiveUfsUri = table.getSd().getLocation();

    try {
      PathTranslator pathTranslator = new PathTranslator();
      ufsUri = new AlluxioURI(table.getSd().getLocation());
      pathTranslator.addMapping(mountAlluxioPath(tableName, ufsUri, alluxioUri), hiveUfsUri);

      for (Partition part : partitions) {
        AlluxioURI partitionUri;
        if (part.getSd() != null && part.getSd().getLocation() != null
            && ufsUri.isAncestorOf(partitionUri = new AlluxioURI(part.getSd().getLocation()))) {
          hiveUfsUri = part.getSd().getLocation();
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
          pathTranslator.addMapping(mountAlluxioPath(tableName, partitionUri, alluxioUri),
              hiveUfsUri);
        }
      }
      return pathTranslator;
    } catch (AlluxioException e) {
      throw new IOException(
          "Failed to mount table location. tableName: " + tableName
              + " hiveUfsLocation: " + hiveUfsUri
              + " AlluxioLocation: " + alluxioUri
              + " error: " + e.getMessage(), e);
    }
  }

  @Override
  public UdbTable getTable(String tableName) throws IOException {
    Table table;
    try {
      table = getHive().getTable(mHiveDbName, tableName);

      // Potentially expensive call
      List<Partition> partitions =
          getHive().listPartitions(mHiveDbName, table.getTableName(), (short) -1);

      PathTranslator pathTranslator = mountAlluxioPaths(table, partitions);
      List<String> colNames = table.getSd().getCols().stream().map(FieldSchema::getName)
          .collect(Collectors.toList());
      List<ColumnStatisticsObj> columnStats =
          getHive().getTableColumnStatistics(mHiveDbName, tableName, colNames);

      List<ColumnStatisticsInfo> colStats =
          columnStats.stream().map(HiveUtils::toProto).collect(Collectors.toList());
      // construct table layout
      PartitionInfo partitionInfo = PartitionInfo.newBuilder()
          .setDbName(getUdbContext().getDbName())
          .setTableName(tableName)
          .addAllDataCols(HiveUtils.toProto(table.getSd().getCols()))
          .setStorage(HiveUtils.toProto(table.getSd(), pathTranslator))
          .putAllParameters(table.getParameters())
          // ignore partition name
          .build();
      Layout layout = Layout.newBuilder()
          .setLayoutType(HiveLayout.TYPE)
          .setLayoutData(partitionInfo.toByteString())
          // ignore spec and statistics for table layout
          .build();

      return new HiveTable(this, pathTranslator, tableName,
          HiveUtils.toProtoSchema(table.getSd().getCols()), colStats,
          HiveUtils.toProto(table.getPartitionKeys()), partitions, layout, table);
    } catch (NoSuchObjectException e) {
      throw new NotFoundException("Table " + tableName + " does not exist.", e);
    } catch (TException e) {
      throw new IOException("Failed to get table: " + tableName + " error: " + e.getMessage(), e);
    }
  }

  IMetaStoreClient getHive() throws IOException {
    if (mHive != null) {
      return mHive;
    }

    // Hive uses/saves the thread context class loader.
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // use the extension class loader
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      HiveConf conf = new HiveConf();
      conf.verifyAndSet("hive.metastore.uris", mConnectionUri);
      mHive =
          RetryingMetaStoreClient.getProxy(conf, NOOP_HOOK, HiveMetaStoreClient.class.getName());
      mHive.getDatabase(mHiveDbName);
      return mHive;
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
}
