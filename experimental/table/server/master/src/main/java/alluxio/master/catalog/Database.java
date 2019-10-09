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

import alluxio.exception.status.NotFoundException;
import alluxio.grpc.catalog.FileStatistics;
import alluxio.grpc.catalog.Schema;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.Catalog;
import alluxio.proto.journal.Journal;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The database implementation that manages a collection of tables.
 */
public class Database {
  private static final Logger LOG = LoggerFactory.getLogger(Database.class);

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
   * @param udbContext the db context
   * @param type the database type
   * @param name the database name
   * @param configuration the configuration
   * @return the database instance
   */
  public static Database create(UdbContext udbContext, String type, String name,
      CatalogConfiguration configuration) {
    UnderDatabase udb = null;
    try {
      udb = udbContext.getUdbRegistry()
          .create(udbContext, type, configuration.getUdbConfiguration(type));
    } catch (IOException e) {
      LOG.info("Creating udb type {} failed, database {} is in disconnected mode", type, name);
    }
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
  public Table getTable(String tableName) throws NotFoundException {
    Table table = mTables.get(tableName);
    if (table == null) {
      throw new NotFoundException("Table " + tableName + " does not exist.");
    }
    return table;
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
   * Returns true if an UDB is connected and can be synced.
   *
   * @return true if there is an UDB backing this database
   */
  public boolean isConnected() {
    return (mUdb != null);
  }

  /**
   * add a table to the database.
   *
   * @param tableName table name
   * @param table table object
   */
  public void addTable(String tableName, Table table) {
    // TODO(gpang): concurrency control
    mTables.put(tableName, table);
  }

  /**
   * Syncs the metadata from the under db.
   * @param context journal context
   */
  public void sync(JournalContext context) throws IOException {
    if (!isConnected()) {
      return;
    }
    for (String tableName : mUdb.getTableNames()) {
      // TODO(gpang): concurrency control
      Table table = mTables.get(tableName);
      if (table == null) {
        // add table from udb
        UdbTable udbTable = mUdb.getTable(tableName);
        table = Table.create(this, udbTable);
        mTables.putIfAbsent(tableName, table);
        // journal the change
        context.get().append(Journal.JournalEntry.newBuilder()
            .setAddTable(Catalog.AddTableEntry.newBuilder().setUdbTable(udbTable.toProto())
                .setDbName(mName).setTableName(tableName)
                .addAllPartitions(table.getPartitions().stream().map(Partition::toProto)
                    .collect(Collectors.toList()))
                .addAllTableStats(table.getStatistics())
                .build()).build());
      } else {
        // sync metadata from udb
        // TODO(gpang): implement
      }
    }
  }
}
