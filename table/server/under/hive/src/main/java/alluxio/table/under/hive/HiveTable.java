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

import alluxio.client.file.FileSystem;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.HiveTableInfo;
import alluxio.grpc.table.ParquetMetadata;
import alluxio.grpc.table.PartitionInfo;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.UdbTableInfo;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.under.hive.util.PathTranslator;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hive table implementation.
 */
public class HiveTable implements UdbTable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTable.class);

  private final HiveDatabase mHiveDatabase;
  private final PathTranslator mPathTranslator;
  private final String mName;
  private final Schema mSchema;
  private final String mBaseLocation;
  private final List<ColumnStatisticsInfo> mStatistics;
  private final List<FieldSchema> mPartitionKeys;
  private final Table mTable;
  private final List<Partition> mPartitions;

  /**
   * Creates a new instance.
   *
   * @param hiveDatabase the hive db
   * @param pathTranslator the path translator
   * @param name the table name
   * @param schema the table schema
   * @param baseLocation the base location
   * @param statistics the table statistics
   * @param cols partition keys
   * @param partitions partition list
   * @param table hive table object
   */
  public HiveTable(HiveDatabase hiveDatabase,
      PathTranslator pathTranslator, String name, Schema schema, String baseLocation,
      List<ColumnStatisticsInfo> statistics, List<FieldSchema> cols, List<Partition> partitions,
      Table table) {
    mHiveDatabase = hiveDatabase;
    mTable = table;
    mPathTranslator = pathTranslator;
    mPartitions = partitions;
    mName = name;
    mSchema = schema;
    mBaseLocation = baseLocation;
    mStatistics = statistics;
    mPartitionKeys = cols;
  }

  private static Map<String, ParquetMetadata> getPartitionMetadata(String path,
      FileSystem alluxioFs) {
    Map<String, ParquetMetadata> metadataMap = new HashMap<>();
    // TODO(yuzhu): clean this up to use proper method to get a list of datafiles
    return metadataMap;
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public Schema getSchema() {
    return mSchema;
  }

  @Override
  public List<ColumnStatisticsInfo> getStatistics() {
    return mStatistics;
  }

  @Override
  public List<UdbPartition> getPartitions() throws IOException {
    List<UdbPartition> udbPartitions = new ArrayList<>();
    try {
      List<Partition> partitions = mPartitions;
      List<String> dataColumns = mTable.getSd().getCols().stream()
          .map(org.apache.hadoop.hive.metastore.api.FieldSchema::getName)
          .collect(Collectors.toList());
      List<String> partitionColumns = mTable.getPartitionKeys().stream()
          .map(org.apache.hadoop.hive.metastore.api.FieldSchema::getName)
          .collect(Collectors.toList());

      List<String> partitionNames = partitions.stream().map(
          partition -> FileUtils.makePartName(partitionColumns,
              partition.getValues())).collect(Collectors.toList());
      Map<String, List<ColumnStatisticsInfo>> statsMap = mHiveDatabase.getHive()
          .getPartitionColumnStatistics(mHiveDatabase.getName(), mName, partitionNames, dataColumns)
          .entrySet().stream().collect(Collectors.toMap(e -> e.getKey(),
              e -> e.getValue().stream().map(HiveUtils::toProto).collect(Collectors.toList()),
              (e1, e2) -> e2));
      for (Partition partition : partitions) {
        String partName = FileUtils.makePartName(partitionColumns, partition.getValues());
        PartitionInfo.Builder pib = PartitionInfo.newBuilder()
            .setDbName(mHiveDatabase.getUdbContext().getDbName()).setTableName(mName)
            .addAllCols(HiveUtils.toProto(partition.getSd().getCols()))
            .setStorage(HiveUtils.toProto(partition.getSd(), mPathTranslator))
            .putAllFileMetadata(getPartitionMetadata(
                mPathTranslator.toAlluxioPath(partition.getSd().getLocation()),
                mHiveDatabase.getUdbContext().getFileSystem()))
            .setPartitionName(partName);
        if (partition.getValues() != null) {
          pib.addAllValues(partition.getValues());
        }
        udbPartitions.add(new HivePartition(new HiveLayout(pib.build(), statsMap.get(partName))));
      }
      return udbPartitions;
    } catch (TException e) {
      throw new IOException(
          "failed to list hive partitions for table: " + mHiveDatabase.getName() + "." + mName, e);
    }
  }

  @Override
  public UdbTableInfo toProto() throws IOException {
    HiveTableInfo.Builder builder = HiveTableInfo.newBuilder();
    builder.setDatabaseName(mHiveDatabase.getUdbContext().getDbName())
        .setTableName(mTable.getTableName())
        .setOwner(mTable.getOwner())
        .setTableType(mTable.getTableType());

    StorageDescriptor sd = mTable.getSd();
    builder.addAllDataColumns(HiveUtils.toProto(mTable.getSd().getCols()))
        .addAllPartitionColumns(HiveUtils.toProto(mTable.getPartitionKeys()))
        .setStorage(HiveUtils.toProto(sd, mPathTranslator))
        .putAllParameters(mTable.getParameters());
    if (mTable.getViewOriginalText() != null) {
      builder.setViewOriginalText(mTable.getViewOriginalText());
    }
    if (mTable.getViewExpandedText() != null) {
      builder.setViewExpandedText(mTable.getViewExpandedText());
    }
    return (UdbTableInfo.newBuilder().setHiveTableInfo(builder.build()).build());
  }
}
