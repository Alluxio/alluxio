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

import alluxio.grpc.catalog.TableInfo;
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
  private final UdbTable mUdbTable;
  private final ArrayList<TableVersion> mVersions;
  // TODO(gpang): this should be indexable by partition spec
  private final ArrayList<Partition> mPartitions;

  private Table(Database database, UdbTable udbTable, List<Partition> partitions) {
    mDatabase = database;
    mUdbTable = udbTable;
    mName = mUdbTable.getName();
    mVersions = new ArrayList<>(2);
    mPartitions = new ArrayList<>(2);
    mPartitions.addAll(partitions);
  }

  /**
   * @param database the database
   * @param udbTable the udb table
   * @return a new instance
   */
  public static Table create(Database database, UdbTable udbTable) throws IOException {
    List<Partition> partitions =
        udbTable.getPartitions2().stream().map(Partition::new).collect(Collectors.toList());
    Table table = new Table(database, udbTable, partitions);

    // add initial version of table
    TableVersion tableVersion = new TableVersion(table, udbTable.getSchema());
    tableVersion.addView(TableVersion.DEFAULT_VIEW_NAME, udbTable.getView());
    table.addVersion(tableVersion);
    return table;
  }

  /**
   * @return the latest version of the table
   */
  public TableVersion get() {
    // TODO(gpang): better version number management
    synchronized (mVersions) {
      return mVersions.get(mVersions.size() - 1);
    }
  }

  /**
   * @return the database
   */
  public Database getDatabase() {
    return mDatabase;
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
   * Adds a new version to the table.
   *
   * @param tableVersion the new table version
   */
  private void addVersion(TableVersion tableVersion) {
    synchronized (mVersions) {
      mVersions.add(tableVersion);
    }
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
    return get().toProto();
  }
}
