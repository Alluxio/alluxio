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
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.TableInfo;
import alluxio.proto.journal.Table.AddTableEntry;
import alluxio.table.common.transform.TransformContext;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;
import alluxio.table.common.udb.UdbTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The table implementation which manages all the versions of the table.
 */
public class Table {
  private static final Logger LOG = LoggerFactory.getLogger(Table.class);

  private String mName;
  private final Database mDatabase;
  private Schema mSchema;
  private String mOwner;

  // partition scheme
  // TODO(gpang): this should be indexable by partition spec
  private List<Partition> mPartitions;
  private List<FieldSchema> mPartitionCols;
  private Layout mLayout;

  private List<ColumnStatisticsInfo> mStatistics;
  private Map<String, String> mParameters;

  private Table(Database database, UdbTable udbTable) {
    mDatabase = database;
    sync(udbTable);
  }

  private Table(Database database, List<Partition> partitions, Schema schema, String tableName,
      String owner, List<ColumnStatisticsInfo> columnStats,
      Map<String, String> parameters, List<FieldSchema> partitionCols, Layout layout) {
    mDatabase = database;
    mName = tableName;
    mSchema = schema;
    mOwner = owner;
    mPartitions = partitions;
    mStatistics = columnStats;
    mParameters = new HashMap<>(parameters);
    mPartitionCols = new ArrayList<>(partitionCols);
    mLayout = layout;
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
      mOwner = udbTable.getOwner();
      mPartitions =
          udbTable.getPartitions().stream().map(Partition::new).collect(Collectors.toList());
      mStatistics = udbTable.getStatistics();
      mParameters = new HashMap<>(udbTable.getParameters());
      mPartitionCols = new ArrayList<>(udbTable.getPartitionCols());
      mLayout = udbTable.getLayout();
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
        entry.getOwner(), entry.getTableStatsList(), entry.getParametersMap(),
        entry.getPartitionColsList(), entry.getLayout());
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
    return mPartitions;
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
    List<TransformPlan> plans = new ArrayList<>(mPartitions.size());
    for (Partition partition : mPartitions) {
      TransformContext transformContext =
          new TransformContext(mDatabase.getName(), mName, partition.getSpec().toString());
      plans.add(partition.getTransformPlan(transformContext, definition));
    }
    return plans;
  }

  /**
   * @return the proto representation
   */
  public TableInfo toProto() {
    TableInfo.Builder builder = TableInfo.newBuilder()
        .setDbName(mDatabase.getName())
        .setTableName(mName)
        .setSchema(mSchema)
        .setOwner(mOwner)
        .putAllParameters(mParameters)
        .addAllPartitionCols(mPartitionCols)
        .setLayout(mLayout);

    return builder.build();
  }

  /**
   * @return the journal proto representation
   */
  public AddTableEntry toJournalProto() {
    AddTableEntry.Builder builder = AddTableEntry.newBuilder()
        .setDbName(mDatabase.getName())
        .setTableName(mName)
        .addAllPartitions(mPartitions.stream().map(Partition::toProto).collect(Collectors.toList()))
        .addAllTableStats(mStatistics)
        .setSchema(mSchema)
        .setOwner(mOwner)
        .putAllParameters(mParameters)
        .addAllPartitionCols(mPartitionCols)
        .setLayout(mLayout);

    return builder.build();
  }
}
