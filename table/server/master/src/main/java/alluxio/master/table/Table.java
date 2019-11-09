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
import alluxio.table.common.UdbPartition;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The table implementation which manages all the versions of the table.
 */
@NotThreadSafe
public class Table {
  private static final Logger LOG = LoggerFactory.getLogger(Table.class);

  private String mName;
  private final Database mDatabase;
  private Schema mSchema;
  private PartitionScheme mPartitionScheme;
  private String mOwner;
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
    mPartitionScheme = PartitionScheme.create(partitions, layout, partitionCols);
    mOwner = owner;
    mStatistics = columnStats;
    mParameters = new HashMap<>(parameters);
  }

  private boolean isSyncable(UdbTable udbTable) {
    if (mSchema == null && mPartitionScheme == null) {
      return true;
    }
    if (!Objects.equals(mSchema, udbTable.getSchema())) {
      // can't sync if the schema is different
      return false;
    }
    if (mPartitionScheme == null || mPartitionScheme.getPartitionCols().isEmpty()) {
      // can't sync if it is non-partitioned table
      return false;
    }
    List<FieldSchema> partitionCols = new ArrayList<>(udbTable.getPartitionCols());
    return Objects.equals(partitionCols, mPartitionScheme.getPartitionCols());
  }

  /**
   * Sync the table with a udbtable.
   *
   * @param udbTable udb table to be synced
   * @return true if the table changed
   */
  public boolean sync(UdbTable udbTable) {
    boolean changed = false;
    try {
      if (!isSyncable(udbTable)) {
        return false;
      }
      // only sync these fields if the table is uninitialized
      if (mName == null) {
        mName = udbTable.getName();
        mSchema = udbTable.getSchema();
        mOwner = udbTable.getOwner();
        mStatistics = udbTable.getStatistics();
        mParameters = new HashMap<>(udbTable.getParameters());
        changed = true;
      }
      List<Partition> partitions = mPartitionScheme == null
          ? new ArrayList<>() : new ArrayList<>(mPartitionScheme.getPartitions());
      Layout tableLayout = mPartitionScheme == null
          ? udbTable.getLayout() : mPartitionScheme.getTableLayout();
      Set<String> partNames = partitions.stream().map(Partition::getSpec)
          .collect(Collectors.toSet());
      for (UdbPartition udbpart : udbTable.getPartitions()) {
        if (!partNames.contains(udbpart.getSpec())) {
          partitions.add(new Partition(udbpart));
          changed = true;
        }
      }
      if (changed) {
        mPartitionScheme = PartitionScheme.create(partitions,
            tableLayout, udbTable.getPartitionCols());
      }
    } catch (IOException e) {
      LOG.info("Sync table {} failed {}", mName, e);
    }
    return changed;
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
   * @param spec the partition spec
   * @return the corresponding partition
   */
  public Partition getPartition(String spec) {
    return mPartitionScheme.getPartition(spec);
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
      if (!partition.isTransformed(definition.getDefinition())) {
        TransformContext transformContext =
            new TransformContext(mDatabase.getName(), mName, partition.getSpec());
        plans.add(partition.getTransformPlan(transformContext, definition));
      }
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
        .addAllPartitionCols(mPartitionScheme.getPartitionCols())
        .setLayout(mPartitionScheme.getTableLayout());

    return builder.build();
  }

  /**
   * @return the journal proto representation
   */
  public AddTableEntry toJournalProto() {
    AddTableEntry.Builder builder = AddTableEntry.newBuilder()
        .setDbName(mDatabase.getName())
        .setTableName(mName)
        .addAllPartitions(getPartitions().stream().map(Partition::toProto)
            .collect(Collectors.toList()))
        .addAllTableStats(mStatistics)
        .setSchema(mSchema)
        .setOwner(mOwner)
        .putAllParameters(mParameters)
        .addAllPartitionCols(mPartitionScheme.getPartitionCols())
        .setLayout(mPartitionScheme.getTableLayout());

    return builder.build();
  }
}
