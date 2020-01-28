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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.table.FileStatistics;
import alluxio.grpc.table.Schema;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The database implementation that manages a collection of tables.
 */
public class Database implements Journaled {
  private static final Logger LOG = LoggerFactory.getLogger(Database.class);

  private final CatalogContext mContext;
  private final String mType;
  private final String mName;
  private final Map<String, Table> mTables;
  private final UnderDatabase mUdb;
  private final Map<String, String> mConfig;

  private DatabaseInfo mDatabaseInfo;

  private Database(CatalogContext context, String type, String name, UnderDatabase udb,
      Map<String, String> configMap) {
    mContext = context;
    mType = type;
    mName = name;
    mTables = new ConcurrentHashMap<>();
    mUdb = udb;
    mConfig = configMap;
  }

  /**
   * Creates an instance of a database.
   *
   * @param catalogContext the catalog context
   * @param udbContext the db context
   * @param type the database type
   * @param name the database name
   * @param configMap the configuration
   * @return the database instance
   */
  public static Database create(CatalogContext catalogContext, UdbContext udbContext, String type,
      String name, Map<String, String> configMap) {
    CatalogConfiguration configuration = new CatalogConfiguration(configMap);
    try {
      UnderDatabase udb = udbContext.getUdbRegistry()
          .create(udbContext, type, configuration.getUdbConfiguration(type));
      return new Database(catalogContext, type, name, udb, configMap);
    } catch (Exception e) {
      throw new IllegalArgumentException("Creating udb failed for database name: " + name, e);
    }
  }

  /**
   * @return the catalog context
   */
  public CatalogContext getContext() {
    return mContext;
  }

  /**
   * @return returns the database name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return returns database info
   */
  public DatabaseInfo getDatabaseInfo() {
    return mDatabaseInfo;
  }

  /**
   * @return returns the database type
   */
  public String getType() {
    return mType;
  }

  /**
   * @return the {@link UnderDatabase}
   */
  public UnderDatabase getUdb() {
    return mUdb;
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
      throw new NotFoundException(ExceptionMessage.TABLE_DOES_NOT_EXIST
          .getMessage(tableName, mName));
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
   *
   * @return the configuration for the database
   */
  public Map<String, String> getConfig() {
    return mConfig;
  }

  /**
   * Syncs the metadata from the under db.
   * @param context journal context
   * @return true if the database changed as a result of fullSync
   */
  public boolean sync(JournalContext context) throws IOException {
    boolean returnVal = false;
    DatabaseInfo newDbInfo = mUdb.getDatabaseInfo();
    if (!newDbInfo.equals(mDatabaseInfo)) {
      applyAndJournal(context, Journal.JournalEntry.newBuilder()
          .setUpdateDatabaseInfo(toJournalProto(newDbInfo, mName)).build());
    }

    for (String tableName : mUdb.getTableNames()) {
      // TODO(gpang): concurrency control
      boolean tableUpdated = false;
      Table table = mTables.get(tableName);
      if (table == null) {
        // add table from udb
        LOG.debug("Importing a new table " + tableName + " into database " + mName);
        UdbTable udbTable = mUdb.getTable(tableName);
        table = Table.create(this, udbTable);
        tableUpdated = true;
      } else {
        LOG.debug("Syncing an existing table " + tableName + " in database " + mName);
        tableUpdated = table.sync(mUdb.getTable(tableName));
      }
      if (tableUpdated) {
        alluxio.proto.journal.Table.AddTableEntry addTableEntry = table.toJournalProto();
        Journal.JournalEntry entry = Journal.JournalEntry.newBuilder().setAddTable(addTableEntry)
            .build();
        applyAndJournal(context, entry);
        returnVal = true;
      }
    }
    return returnVal;
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    if (entry.hasAddTable()) {
      alluxio.proto.journal.Table.AddTableEntry addTable = entry.getAddTable();
      if (addTable.getDbName().equals(mName)) {
        apply(addTable);
        return true;
      }
    }
    if (entry.hasUpdateDatabaseInfo()) {
      alluxio.proto.journal.Table.UpdateDatabaseInfoEntry updateDb = entry.getUpdateDatabaseInfo();
      if (updateDb.getDbName().equals(mName)) {
        apply(updateDb);
        return true;
      }
    }
    return false;
  }

  private void apply(alluxio.proto.journal.Table.UpdateDatabaseInfoEntry updateDb) {
    mDatabaseInfo = new DatabaseInfo(updateDb.getLocation(), updateDb.getOwnerName(),
        updateDb.getOwnerType(), updateDb.getComment(), updateDb.getParameterMap());
  }

  private void apply(alluxio.proto.journal.Table.AddTableEntry entry) {
    Table table = Table.create(this, entry);
    addTable(entry.getTableName(), table);
  }

  @Override
  public void resetState() {
    mTables.clear();
  }

  private Iterator<Journal.JournalEntry> getTableIterator() {
    final Iterator<Table> it = getTables().iterator();
    return new Iterator<Journal.JournalEntry>() {
      private Table mEntry = null;

      @Override
      public boolean hasNext() {
        if (mEntry != null) {
          return true;
        }
        if (it.hasNext()) {
          mEntry = it.next();
          return true;
        }
        return false;
      }

      @Override
      public Journal.JournalEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Table table = mEntry;
        mEntry = null;
        alluxio.proto.journal.Table.AddTableEntry addTableEntry = table.toJournalProto();
        return Journal.JournalEntry.newBuilder().setAddTable(addTableEntry).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "GetTableIteratorr#Iterator#remove is not supported.");
      }
    };
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    Journal.JournalEntry entry = Journal.JournalEntry.newBuilder().setUpdateDatabaseInfo(
        toJournalProto(getDatabaseInfo(), mName)).build();
    return Iterators.concat(Iterators.singletonIterator(entry), getTableIterator());
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.TABLE_MASTER_DATABASE;
  }

  /**
   * @param dbInfo database info object
   * @param dbName database name
   * @return the journal proto representation
   */
  public static alluxio.proto.journal.Table.UpdateDatabaseInfoEntry toJournalProto(
      DatabaseInfo dbInfo, String dbName) {
    alluxio.proto.journal.Table.UpdateDatabaseInfoEntry.Builder builder =
        alluxio.proto.journal.Table.UpdateDatabaseInfoEntry.newBuilder()
            .setOwnerName(dbInfo.getOwnerName()).setOwnerType(dbInfo.getOwnerType())
            .setDbName(dbName).putAllParameter(dbInfo.getParameters());
    if (dbInfo.getComment() != null) {
      builder.setComment(dbInfo.getComment());
    }
    if (dbInfo.getLocation() != null) {
      builder.setLocation(dbInfo.getLocation());
    }
    return builder.build();
  }
}
