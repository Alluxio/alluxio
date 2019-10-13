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
import alluxio.table.common.udb.UdbTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private UdbTableInfo mTableInfo;
  // TODO(gpang): this should be indexable by partition spec
  private List<Partition> mPartitions;
  private List<ColumnStatisticsInfo> mStatistics;

  private Table(Database database, UdbTable udbTable) {
    mDatabase = database;
    sync(udbTable);
  }

  private Table(Database database, List<Partition> partitions, Schema schema,
      String tableName, List<ColumnStatisticsInfo> columnStats) {
    mDatabase = database;
    mName = tableName;
    mSchema = schema;
    mPartitions = partitions;
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
      mPartitions =
          udbTable.getPartitions().stream().map(Partition::new).collect(Collectors.toList());
      mTableInfo = udbTable.toProto();
      mStatistics = udbTable.getStatistics();
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
        entry.getTableStatsList());
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
   * @return the proto representation
   */
  public TableInfo toProto() {
    TableInfo.Builder builder = TableInfo.newBuilder()
        .setDbName(mDatabase.getName())
        .setTableName(mName)
        .setSchema(mSchema);

    builder.setUdbInfo(mTableInfo);
    return builder.build();
  }
}
