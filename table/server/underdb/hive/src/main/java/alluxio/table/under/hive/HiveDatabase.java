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
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.master.table.DatabaseInfo;
import alluxio.resource.CloseableResource;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.udb.PathTranslator;
import alluxio.table.common.udb.UdbInExClusionSpec;
import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UdbUtils;
import alluxio.table.common.udb.UnderDatabase;
import alluxio.table.under.hive.util.HiveClientPoolCache;
import alluxio.table.under.hive.util.HiveClientPool;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Hive database implementation.
 */
public class HiveDatabase implements UnderDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(HiveDatabase.class);

  private static final int MAX_PARTITION_COLUMN_STATISTICS = 10000;

  private final UdbContext mUdbContext;
  private final UdbConfiguration mConfiguration;
  /** the connection uri for the hive metastore. */
  private final String mConnectionUri;
  /** the name of the hive db. */
  private final String mHiveDbName;
  /** path translator that records mappings between ufs paths and Alluxio paths. */
  private final PathTranslator mPathTranslator;
  private final AtomicBoolean mIsMounted;

  private static final HiveClientPoolCache CLIENT_POOL_CACHE = new HiveClientPoolCache();
  /** Hive client is not thread-safe, so use a client pool for concurrency. */
  private final HiveClientPool mClientPool;

  private HiveDatabase(UdbContext udbContext, UdbConfiguration configuration,
      String connectionUri, String hiveDbName) {
    mUdbContext = udbContext;
    mConfiguration = configuration;
    mConnectionUri = connectionUri;
    mHiveDbName = hiveDbName;
    mClientPool = CLIENT_POOL_CACHE.getPool(connectionUri);
    mPathTranslator = new PathTranslator();
    mIsMounted = new AtomicBoolean(false);
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
    try (CloseableResource<IMetaStoreClient> client = mClientPool.acquireClientResource()) {
      Database hiveDb = client.get().getDatabase(mHiveDbName);
      alluxio.grpc.table.PrincipalType type = alluxio.grpc.table.PrincipalType.USER;
      if (Objects.equals(hiveDb.getOwnerType(), PrincipalType.ROLE)) {
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
    try (CloseableResource<IMetaStoreClient> client = mClientPool.acquireClientResource()) {
      return client.get().getAllTables(mHiveDbName);
    } catch (TException  e) {
      throw new IOException("Failed to get hive tables: " + e.getMessage(), e);
    }
  }

  /**
   * Mounts the database to Alluxio filesystem. This method mounts tables and partitions
   * (if applicable) within this database to Alluxio filesystem.
   *
   * @param bypassSpec bypass spec
   */
  private void mount(UdbInExClusionSpec bypassSpec) throws IOException {
    List<String> tableNames = getTableNames().stream()
        // ignored tables should not be mounted
        .filter((tableName) -> !bypassSpec.hasIgnoredTable(tableName))
        .collect(Collectors.toList());
    Map<Table, List<Partition>> tables = new HashMap<>(tableNames.size());
    for (String tableName : tableNames) {
      try (CloseableResource<IMetaStoreClient> client = mClientPool.acquireClientResource()) {
        Table table = client.get().getTable(mHiveDbName, tableName);
        List<Partition> partitions =
            client.get().listPartitions(mHiveDbName, table.getTableName(), (short) -1);
        tables.put(table, partitions);
      } catch (TException e) {
        LOG.error(
            "Failed to get table {} and partitions of database {}", tableName, mHiveDbName);
      }
    }

    mount(new ArrayList<>(tables.keySet()), bypassSpec);
    for (Map.Entry<Table, List<Partition>> entry : tables.entrySet()) {
      Table table = entry.getKey();
      List<Partition> partitions = entry.getValue();
      mount(table, partitions, bypassSpec);
    }
  }

  private void mount(List<Table> tables, UdbInExClusionSpec bypassSpec)
      throws IOException {
    HashSet<AlluxioURI> fragmentRootUfsUris = new HashSet<>();
    for (Table table : tables) {
      if (table.getSd() == null || table.getSd().getLocation() == null) {
        continue;
      }
      String tableUfsPath = table.getSd().getLocation();
      if (bypassSpec.hasFullyBypassedTable(table.getTableName())) {
        mPathTranslator.addMapping(tableUfsPath, tableUfsPath);
        continue;
      }
      AlluxioURI tableUfsUri = new AlluxioURI(tableUfsPath);
      AlluxioURI rootUfsUri = new AlluxioURI(tableUfsUri, "/", false);
      fragmentRootUfsUris.add(rootUfsUri);
    }
    for (AlluxioURI rootUfsUri : fragmentRootUfsUris) {
      mountFragmentAndAddMapping(rootUfsUri);
    }
  }

  private void mount(Table table, List<Partition> partitions, UdbInExClusionSpec bypassSpec)
      throws IOException {
    final String tableName = table.getTableName();
    final String tableUfsPath = table.getSd().getLocation();
    final AlluxioURI tableUfsUri = new AlluxioURI(tableUfsPath);
    final AlluxioURI rootUfsUri = new AlluxioURI(tableUfsUri, "/", false);

    for (Partition part : partitions) {
      if (part.getSd() == null || part.getSd().getLocation() == null) {
        continue;
      }
      String partitionUfsPath = part.getSd().getLocation();
      AlluxioURI partitionUfsUri = new AlluxioURI(partitionUfsPath);
      String partName = makePartName(table, part);
      if (bypassSpec.hasBypassedPartition(tableName, partName)) {
        mPathTranslator.addMapping(partitionUfsPath, partitionUfsPath);
        continue;
      }
      boolean isAncestor;
      try {
        isAncestor = rootUfsUri.isAncestorOf(partitionUfsUri);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      }
      if (!isAncestor && mConfiguration.getBoolean(Property.ALLOW_DIFF_PART_LOC_PREFIX)) {
        // case 1: partition is NOT located on the same physical node with the parent table,
        // but config says mount it anyway
        // action: mount its containing fragment
        mountFragmentAndAddMapping(rootUfsUri);
      }
      // partition on the same node,
      // or config not allowing mounting a partition on a different node
      // action: ignore it
    }
  }

  private void mountFragmentAndAddMapping(AlluxioURI rootUfsUri)
      throws IOException {
    AlluxioURI fragmentAlluxioUri = mUdbContext.getFragmentLocation(rootUfsUri);
    try {
      mPathTranslator.addMapping(
          UdbUtils.mountFragment(mHiveDbName,
              rootUfsUri,
              fragmentAlluxioUri,
              mUdbContext,
              mConfiguration),
          rootUfsUri.toString()
      );
    } catch (AlluxioException e) {
      throw new IOException(String.format(
          "Failed to mount database fragment. "
              + "hiveUfsLocation: %s, AlluxioLocation: %s, error: %s",
          rootUfsUri, fragmentAlluxioUri, e.getMessage()),
          e);
    }
  }

  private static String makePartName(Table table, Partition partition) {
    String partName = partition.getValues().toString();
    try {
      partName = Warehouse.makePartName(table.getPartitionKeys(), partition.getValues());
    } catch (MetaException e) {
      LOG.warn("Error making partition name for table {}, partition {}", table.getTableName(),
          partition.getValues().toString());
    }
    return partName;
  }

  @Override
  public UdbTable getTable(String tableName, UdbInExClusionSpec bypassSpec) throws IOException {
    try {
      Table table;
      List<Partition> partitions;
      List<ColumnStatisticsObj> columnStats;
      List<String> partitionColumns;
      Map<String, List<ColumnStatisticsInfo>> statsMap = new HashMap<>();
      // perform all the hive client operations, and release the client early.
      try (CloseableResource<IMetaStoreClient> client = mClientPool.acquireClientResource()) {
        table = client.get().getTable(mHiveDbName, tableName);

        // Potentially expensive call
        partitions = client.get().listPartitions(mHiveDbName, table.getTableName(), (short) -1);

        List<String> colNames = table.getSd().getCols().stream().map(FieldSchema::getName)
            .collect(Collectors.toList());
        columnStats = client.get().getTableColumnStatistics(mHiveDbName, tableName, colNames);

        // construct the partition statistics
        List<String> dataColumns = table.getSd().getCols().stream()
            .map(org.apache.hadoop.hive.metastore.api.FieldSchema::getName)
            .collect(Collectors.toList());
        partitionColumns = table.getPartitionKeys().stream()
            .map(org.apache.hadoop.hive.metastore.api.FieldSchema::getName)
            .collect(Collectors.toList());
        List<String> partitionNames = partitions.stream()
            .map(partition -> FileUtils.makePartName(partitionColumns, partition.getValues()))
            .collect(Collectors.toList());

        for (List<String> partialPartitionNames :
            Lists.partition(partitionNames, MAX_PARTITION_COLUMN_STATISTICS)) {
          statsMap.putAll(client.get()
              .getPartitionColumnStatistics(mHiveDbName, tableName,
                  partialPartitionNames, dataColumns)
              .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                  e -> e.getValue().stream().map(HiveUtils::toProto).collect(Collectors.toList()),
                  (e1, e2) -> e2)));
        }
      }

      if (mIsMounted.compareAndSet(false, true)) {
        // TODO(bowen): bypass spec updates are ignored
        mount(bypassSpec);
      }
      List<ColumnStatisticsInfo> colStats =
          columnStats.stream().map(HiveUtils::toProto).collect(Collectors.toList());
      // construct table layout
      PartitionInfo partitionInfo = PartitionInfo.newBuilder()
          .setDbName(getUdbContext().getDbName())
          .setTableName(tableName)
          .addAllDataCols(HiveUtils.toProto(table.getSd().getCols()))
          .setStorage(HiveUtils.toProto(table.getSd(), mPathTranslator))
          .putAllParameters(table.getParameters())
          // ignore partition name
          .build();
      Layout layout = Layout.newBuilder()
          .setLayoutType(HiveLayout.TYPE)
          .setLayoutData(partitionInfo.toByteString())
          // ignore spec and statistics for table layout
          .build();

      // create udb partitions info
      List<UdbPartition> udbPartitions = new ArrayList<>();
      if (partitionColumns.isEmpty()) {
        // unpartitioned table, generate a partition
        PartitionInfo.Builder pib = PartitionInfo.newBuilder()
            .setDbName(getUdbContext().getDbName())
            .setTableName(tableName)
            .addAllDataCols(HiveUtils.toProto(table.getSd().getCols()))
            .setStorage(HiveUtils.toProto(table.getSd(), mPathTranslator))
            .setPartitionName(tableName)
            .putAllParameters(table.getParameters());
        udbPartitions.add(new HivePartition(
            new HiveLayout(pib.build(), Collections.emptyList())));
      } else {
        for (Partition partition : partitions) {
          String partName = FileUtils.makePartName(partitionColumns, partition.getValues());
          PartitionInfo.Builder pib = PartitionInfo.newBuilder()
              .setDbName(getUdbContext().getDbName())
              .setTableName(tableName)
              .addAllDataCols(HiveUtils.toProto(partition.getSd().getCols()))
              .setStorage(HiveUtils.toProto(partition.getSd(), mPathTranslator))
              .setPartitionName(partName)
              .putAllParameters(partition.getParameters());
          if (partition.getValues() != null) {
            pib.addAllValues(partition.getValues());
          }
          udbPartitions.add(new HivePartition(new HiveLayout(pib.build(),
              statsMap.getOrDefault(partName, Collections.emptyList()))));
        }
      }

      return new HiveTable(tableName, HiveUtils.toProtoSchema(table.getSd().getCols()), colStats,
          HiveUtils.toProto(table.getPartitionKeys()), udbPartitions, layout, table);
    } catch (NoSuchObjectException e) {
      throw new NotFoundException("Table " + tableName + " does not exist.", e);
    } catch (TException e) {
      throw new IOException("Failed to get table: " + tableName + " error: " + e.getMessage(), e);
    }
  }
}
