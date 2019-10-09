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

package alluxio.master.catalog;

import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.Schema;
import alluxio.grpc.catalog.TableInfo;
import alluxio.grpc.catalog.UdbTableInfo;
import alluxio.proto.journal.Catalog;
import alluxio.table.common.udb.UdbTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The table implementation which manages all the versions of the table.
 */
public class Table {
  private final String mName;
  private final Database mDatabase;
  private final Schema mSchema;
  private UdbTableInfo mTableInfo;
  // TODO(gpang): this should be indexable by partition spec
  private List<Partition> mPartitions;

  private Table(Database database, UdbTable udbTable, List<Partition> partitions) {
    mDatabase = database;
    mUdbTable = udbTable;
    mName = mUdbTable.getName();
    mSchema = mUdbTable.getSchema();
    mPartitions = new ArrayList<>(partitions);
  }

  private Table(Database database, List<Partition> partitions, Schema schema,
      String tableName, List<ColumnStatisticsInfo> columnStats) {
    mDatabase = database;
    mName = tableName
    mSchema = schema
    mPartitions = new ArrayList<>(partitions);
    mUdbTable = null;
  }

  public void sync(UdbTable udbTable) throws IOException {
    mName = mUdbTable.getName();
    mSchema = mUdbTable.getSchema();
    mPartitions =
        udbTable.getPartitions().stream().map(Partition::new).collect(Collectors.toList());
  }

  /**
   * @param database the database
   * @param udbTable the udb table
   * @return a new instance
   */
  public static Table create(Database database, UdbTable udbTable) throws IOException {
    List<Partition> partitions =
        udbTable.getPartitions().stream().map(Partition::new).collect(Collectors.toList());
    return new Table(database, udbTable, partitions);
  }

  public static Table create(Database database, Catalog.AddTableEntry entry) {
    return new Table(database, entry.getPartitionsList(), entry.getUdbTable().getHiveTableInfo().get)
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
    return mUdbTable.getStatistics();
  }

  /**
   * Returns the udb table.
   *
   * @return udb table
   */
  public UdbTable getUdbTable() {
    return mUdbTable;
  }

  /**
   * @return the proto representation
   */
  public TableInfo toProto() throws IOException {
    TableInfo.Builder builder = TableInfo.newBuilder()
        .setDbName(mDatabase.getName())
        .setTableName(mName)
        .setSchema(mSchema);

    builder.setUdbInfo(mUdbTable.toProto());
    return builder.build();
  }
}
