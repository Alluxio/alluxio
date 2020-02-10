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

import alluxio.Constants;
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
import alluxio.util.CommonUtils;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.annotation.Nullable;

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
   *
   * @return the configuration for the database
   */
  public Map<String, String> getConfig() {
    return mConfig;
  }

  /**
   * Syncs the metadata from the under db. To avoid concurrent sync operations, this requires
   * external synchronization.
   *
   * @param context journal context
   * @param service executor service for parallezing the sync
   * @return true if the database changed as a result of syncing with UDB
   */
  public boolean sync(JournalContext context, ExecutorService service) throws IOException {
    AtomicBoolean updated = new AtomicBoolean(false);
    DatabaseInfo newDbInfo = mUdb.getDatabaseInfo();
    if (!newDbInfo.equals(mDatabaseInfo)) {
      applyAndJournal(context, Journal.JournalEntry.newBuilder()
          .setUpdateDatabaseInfo(toJournalProto(newDbInfo, mName)).build());
      updated.set(true);
    }
    Set<String> udbTableNames = new HashSet<>(mUdb.getTableNames());

    // sync each table in parallel, with the executor service
    List<Callable<Void>> tasks = new ArrayList<>(udbTableNames.size());
    final Database thisDb = this;
    for (String tableName : udbTableNames) {
      tasks.add(() -> {
        Table previousTable = mTables.get(tableName);
        UdbTable udbTable = mUdb.getTable(tableName);
        Table newTable = Table.create(thisDb, udbTable, previousTable);

        if (newTable != null) {
          // table was created or was updated
          alluxio.proto.journal.Table.AddTableEntry addTableEntry = newTable.toJournalProto();
          Journal.JournalEntry entry =
              Journal.JournalEntry.newBuilder().setAddTable(addTableEntry).build();
          applyAndJournal(context, entry);
          updated.set(true);
        }
        return null;
      });
    }

    try {
      CommonUtils.invokeAll(service, tasks, 10 * Constants.MINUTE_MS);
    } catch (Exception e) {
      throw new IOException("Failed to sync database " + mName, e);
    }

    for (Table existingTable : mTables.values()) {
      if (!udbTableNames.contains(existingTable.getName())) {
        // this table no longer exists in udb
        alluxio.proto.journal.Table.RemoveTableEntry removeTableEntry =
            alluxio.proto.journal.Table.RemoveTableEntry.newBuilder()
                .setDbName(mName)
                .setTableName(existingTable.getName())
                .setVersion(existingTable.getVersion())
                .build();
        Journal.JournalEntry entry = Journal.JournalEntry.newBuilder()
            .setRemoveTable(removeTableEntry)
            .build();
        applyAndJournal(context, entry);
        updated.set(true);
      }
    }
    return updated.get();
  }

  @Override
  public void applyAndJournal(Supplier<JournalContext> context, Journal.JournalEntry entry) {
    // This is journaled differently from others components, since optimistic concurrency control
    // is utilized. There are no external locks for the table, so the locking will happen during
    // the access of the tables map.
    processJournalEntryInternal(entry, context.get());
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    // Do not journal when processing journal entries
    return processJournalEntryInternal(entry, null);
  }

  /**
   * @param entry the journal entry to process
   * @param context the journal context, will not journal if null
   * @return whether the entry type is supported by this journaled object
   */
  private boolean processJournalEntryInternal(Journal.JournalEntry entry,
      @Nullable JournalContext context) {
    if (entry.hasAddTable()) {
      return applyAddTable(context, entry);
    }
    if (entry.hasRemoveTable()) {
      return applyRemoveTable(context, entry);
    }
    if (entry.hasUpdateDatabaseInfo()) {
      return applyUpdateDbInfo(context, entry);
    }
    return false;
  }

  private boolean applyUpdateDbInfo(@Nullable JournalContext context, Journal.JournalEntry entry) {
    alluxio.proto.journal.Table.UpdateDatabaseInfoEntry updateDb = entry.getUpdateDatabaseInfo();
    if (!updateDb.getDbName().equals(mName)) {
      return false;
    }
    if (context != null) {
      context.append(entry);
    }
    mDatabaseInfo = new DatabaseInfo(updateDb.getLocation(), updateDb.getOwnerName(),
        updateDb.getOwnerType(), updateDb.getComment(), updateDb.getParameterMap());
    return true;
  }

  private boolean applyAddTable(@Nullable JournalContext context, Journal.JournalEntry entry) {
    alluxio.proto.journal.Table.AddTableEntry addTable = entry.getAddTable();
    if (!addTable.getDbName().equals(mName)) {
      return false;
    }

    Table newTable = Table.create(this, addTable);
    mTables.compute(newTable.getName(), (key, existingTable) -> {
      boolean writeNewTable = false;
      if (existingTable == null && (newTable.getVersion() == Table.FIRST_VERSION)) {
        // this table is being newly inserted, and has the expected first version
        LOG.info("Adding new table {}.{}", mName, newTable.getName());
        writeNewTable = true;
      }

      if (existingTable != null && (newTable.getPreviousVersion() == existingTable.getVersion())) {
        // Previous table already exists, and matches the new table's previous version
        LOG.info("Updating table {}.{} to version {}", mName, newTable.getName(),
            newTable.getVersion());
        writeNewTable = true;
      }

      if (writeNewTable) {
        // The new table has been successfully validated, so update the map with the new table,
        // and journal the entry if the journal context exists.
        if (context != null) {
          context.append(entry);
        }
        return newTable;
      } else {
        // The table to add does not validate with the existing table, so another thread must
        // have updated the map. Do not modify the map.
        return existingTable;
      }
    });

    return true;
  }

  private boolean applyRemoveTable(@Nullable JournalContext context, Journal.JournalEntry entry) {
    alluxio.proto.journal.Table.RemoveTableEntry removeTable = entry.getRemoveTable();
    if (!removeTable.getDbName().equals(mName)) {
      return false;
    }

    mTables.compute(removeTable.getTableName(), (key, existingTable) -> {
      if (existingTable != null) {
        if (removeTable.getVersion() == existingTable.getVersion()) {
          // this table is being removed, and has the expected version
          LOG.info("Removing table {}.{}", mName, removeTable.getTableName());
          if (context != null) {
            context.append(entry);
          }
          return null;
        }
        LOG.info("Will not remove table {}.{}, because of mismatched versions. "
                + "version-to-delete: {} existing-version: {}", mName, removeTable.getTableName(),
            removeTable.getVersion(), existingTable.getVersion());
      }
      LOG.debug("Cannot remove table {}.{}, because it does not exist.", mName,
          removeTable.getTableName());
      return existingTable;
    });

    return true;
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
            .setDbName(dbName).putAllParameter(dbInfo.getParameters());
    if (dbInfo.getComment() != null) {
      builder.setComment(dbInfo.getComment());
    }
    if (dbInfo.getLocation() != null) {
      builder.setLocation(dbInfo.getLocation());
    }
    if (dbInfo.getOwnerName() != null) {
      builder.setOwnerName(dbInfo.getOwnerName());
    }
    if (dbInfo.getOwnerType() != null) {
      builder.setOwnerType(dbInfo.getOwnerType());
    }
    return builder.build();
  }
}
