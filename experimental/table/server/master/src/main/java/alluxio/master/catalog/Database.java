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

import alluxio.grpc.FileStatistics;
import alluxio.grpc.Schema;
import alluxio.table.common.UdbTable;
import alluxio.table.common.UnderDatabase;
import alluxio.table.common.UnderDatabaseRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The database implementation that manages a collection of tables.
 */
public class Database {
  private final String mType;
  private final String mName;
  private final Map<String, Table> mTables;
  private final UnderDatabase mUdb;

  /**
   * Creates an instance of a database.
   *
   * @param type the database type
   * @param name the database name
   * @param udb the udb
   */
  private Database(String type, String name, UnderDatabase udb) {
    mType = type;
    mName = name;
    mTables = new ConcurrentHashMap<>();
    mUdb = udb;
  }

  /**
   * Creates an instance of a database.
   *
   * @param udbRegistry the udb registry
   * @param type the database type
   * @param name the database name
   * @return the database instance
   */
  public static Database create(UnderDatabaseRegistry udbRegistry, String type, String name)
      throws IOException {
    UnderDatabase udb = udbRegistry.create(type, Collections.emptyMap());
    return new Database(type, name, udb);
  }

  /**
   * @return returns the database name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return returns the database type
   */
  public String getType() {
    return mType;
  }

  /**
   * @return the list of all tables
   */
  public List<Table> getTables() {
    return new ArrayList<>(mTables.values());
  }

  /**
   * @param tableName the table name
   * @return the {@link Table} for the specified table name
   */
  public Table getTable(String tableName) {
    return mTables.get(tableName);
  }

  /**
   * Creates a new table within this database.
   *
   * @param tableName the new table name
   * @param schema the schema for the table
   * @return the {@link Table} for the newly created table
   */
  public Table createTable(String tableName, Schema schema) {
    // TODO(gpang): implement
    return mTables.get(tableName);
  }

  /**
   * @param tableName the table name
   * @return statistics for the specified table name
   */
  public Map<String, FileStatistics> getStatistics(String tableName) {
    // TODO(gpang): implement
    return Collections.emptyMap();
  }

  /**
   * Syncs the metadata from the under db.
   */
  public void sync() throws IOException {
    for (String tableName : mUdb.getTableNames()) {
      // TODO(gpang): concurrency control
      Table table = mTables.get(tableName);
      if (table == null) {
        // add table from udb
        UdbTable udbTable = mUdb.getTable(tableName);
        mTables.putIfAbsent(tableName, new Table(udbTable.getName(), udbTable.getSchema(),
            udbTable.getBaseLocation(), udbTable.getStatistics()));
      } else {
        // sync metadata from udb
        // TODO(gpang): implement
      }
    }
  }
}
