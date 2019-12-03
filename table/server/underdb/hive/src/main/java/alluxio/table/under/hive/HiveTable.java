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

import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.under.hive.util.PathTranslator;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
  private final String mOwner;
  private final List<ColumnStatisticsInfo> mStatistics;
  private final List<FieldSchema> mPartitionKeys;
  private final Table mTable;
  private final List<Partition> mPartitions;
  private final Map<String, String> mParameters;
  private final Layout mLayout;

  /**
   * Creates a new instance.
   *
   * @param hiveDatabase the hive db
   * @param pathTranslator the path translator
   * @param name the table name
   * @param schema the table schema
   * @param statistics the table statistics
   * @param cols partition keys
   * @param partitions partition list
   * @param layout the table layout
   * @param table hive table object
   */
  public HiveTable(HiveDatabase hiveDatabase, PathTranslator pathTranslator, String name,
      Schema schema, List<ColumnStatisticsInfo> statistics, List<FieldSchema> cols,
      List<Partition> partitions, Layout layout, Table table) {
    mHiveDatabase = hiveDatabase;
    mTable = table;
    mPathTranslator = pathTranslator;
    mPartitions = partitions;
    mName = name;
    mSchema = schema;
    mStatistics = statistics;
    mPartitionKeys = cols;
    mOwner = table.getOwner();
    mParameters = (table.getParameters() != null) ? table.getParameters() : Collections.emptyMap();
    mLayout = layout;
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
  public String getOwner() {
    return mOwner;
  }

  @Override
  public Map<String, String> getParameters() {
    return mParameters;
  }

  @Override
  public List<FieldSchema> getPartitionCols() {
    return mPartitionKeys;
  }

  @Override
  public List<ColumnStatisticsInfo> getStatistics() {
    return mStatistics;
  }

  @Override
  public Layout getLayout() {
    return mLayout;
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

      if (partitionColumns.isEmpty()) {
        // unpartitioned table, generate a partition
        PartitionInfo.Builder pib = PartitionInfo.newBuilder()
            .setDbName(mHiveDatabase.getUdbContext().getDbName()).setTableName(mName)
            .addAllDataCols(HiveUtils.toProto(mTable.getSd().getCols()))
            .setStorage(HiveUtils.toProto(mTable.getSd(), mPathTranslator))
            .setPartitionName(mName)
            .putAllParameters(mTable.getParameters());
        udbPartitions.add(new HivePartition(
            new HiveLayout(pib.build(), Collections.emptyList())));
        return udbPartitions;
      }

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
            .setDbName(mHiveDatabase.getUdbContext().getDbName())
            .setTableName(mName)
            .addAllDataCols(HiveUtils.toProto(partition.getSd().getCols()))
            .setStorage(HiveUtils.toProto(partition.getSd(), mPathTranslator))
            .setPartitionName(partName)
            .putAllParameters(partition.getParameters());
        if (partition.getValues() != null) {
          pib.addAllValues(partition.getValues());
        }
        udbPartitions.add(new HivePartition(
            new HiveLayout(pib.build(), statsMap.getOrDefault(partName, Collections.emptyList()))));
      }
      return udbPartitions;
    } catch (TException e) {
      throw new IOException(
          "failed to list hive partitions for table: " + mHiveDatabase.getName() + "." + mName, e);
    }
  }
}
