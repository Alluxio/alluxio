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

package alluxio.master.table;

import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.TableInfo;
import alluxio.grpc.table.UdbTableInfo;
import alluxio.table.common.transform.TransformContext;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;
import alluxio.table.common.udb.UdbTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The table implementation which manages all the versions of the table.
 */
public class Table {
  private static final Logger LOG = LoggerFactory.getLogger(Table.class);

  private String mName;
  private final Database mDatabase;
  private Schema mSchema;
  private PartitionScheme mPartitionScheme;
  // TODO(gpang): this should be indexable by partition spec
  private List<ColumnStatisticsInfo> mStatistics;
  private boolean mIsPartitioned;

  private Table(Database database, UdbTable udbTable) {
    mDatabase = database;
    sync(udbTable);
  }

  private Table(Database database, List<Partition> partitions, Schema schema,
      String tableName, UdbTableInfo tableInfo, List<ColumnStatisticsInfo> columnStats,
      boolean isPartitioned) {
    mDatabase = database;
    mName = tableName;
    mSchema = schema;
    mIsPartitioned = isPartitioned;
    mPartitionScheme = PartitionScheme.createPartitionScheme(partitions, tableInfo, mIsPartitioned);
    mStatistics = columnStats;
  }

  /**
   * sync the table with a udbtable.
   *
   * @param udbTable udb table to be synced
   */
  public void sync(UdbTable udbTable) {
    try {
      mName = udbTable.getName();
      mSchema = udbTable.getSchema();
      List<Partition> partitions =
          udbTable.getPartitions().stream().map(Partition::new).collect(Collectors.toList());
      UdbTableInfo tableInfo = udbTable.toProto();
      mStatistics = udbTable.getStatistics();
      mIsPartitioned = !udbTable.getPartitionKeys().isEmpty();
      mPartitionScheme = PartitionScheme.createPartitionScheme(partitions,
          tableInfo, mIsPartitioned);
    } catch (IOException e) {
      LOG.info("Sync table {} failed {}", mName, e);
    }
  }

  /**
   * @param database the database
   * @param udbTable the udb table
   * @return a new instance
   */
  public static Table create(Database database, UdbTable udbTable) throws IOException {
    return new Table(database, udbTable);
  }

  /**
   * @param database the database
   * @param entry the add table entry
   * @return a new instance
   */
  public static Table create(Database database, alluxio.proto.journal.Table.AddTableEntry entry) {
    List<Partition> partitions = entry.getPartitionsList().stream()
        .map(p -> Partition.fromProto(database.getContext().getLayoutRegistry(), p))
        .collect(Collectors.toList());

    return new Table(database, partitions, entry.getSchema(), entry.getTableName(),
        entry.getUdbTable(), entry.getTableStatsList(), entry.getPartitioned());
  }
  /**
   * @return the table name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the list of partitions
   */
  public List<Partition> getPartitions() {
    return mPartitionScheme.getPartitions();
  }

  /**
   * @return the table schema
   */
  public Schema getSchema() {
    return mSchema;
  }

  /**
   * @return the statistics
   */
  public List<ColumnStatisticsInfo> getStatistics() {
    return mStatistics;
  }

  /**
   * Returns a list of plans to transform the table, according to the transformation definition.
   *
   * @param definition the transformation definition
   * @return a list of {@link TransformPlan} to transform this table
   */
  public List<TransformPlan> getTransformPlans(TransformDefinition definition) throws IOException {
    List<TransformPlan> plans = new ArrayList<>(getPartitions().size());
    for (Partition partition : getPartitions()) {
      TransformContext transformContext =
          new TransformContext(mDatabase.getName(), mName, partition.getSpec().toString());
      plans.add(partition.getTransformPlan(transformContext, definition));
    }
    return plans;
  }

  /**
   * @return if the table is partitioned
   */
  public boolean isPartitioned() {
    return mIsPartitioned;
  }

  /**
   * @return the proto representation
   */
  public TableInfo toProto() {
    TableInfo.Builder builder = TableInfo.newBuilder()
        .setDbName(mDatabase.getName())
        .setTableName(mName)
        .setSchema(mSchema);

    builder.setUdbInfo(mPartitionScheme.getTableLayout());
    return builder.build();
  }
}
