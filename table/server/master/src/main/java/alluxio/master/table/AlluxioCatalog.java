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

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.ColumnStatisticsList;
import alluxio.grpc.table.Constraint;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;
import alluxio.table.common.LayoutRegistry;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UnderDatabaseRegistry;
import alluxio.util.StreamUtils;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Class representing the Alluxio catalog.
 */
public class AlluxioCatalog implements Journaled {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioCatalog.class);

  private final Map<String, Database> mDBs = new ConcurrentHashMap<>();
  private final UnderDatabaseRegistry mUdbRegistry;
  private final LayoutRegistry mLayoutRegistry;
  private final FileSystem mFileSystem;

  /**
   * Creates an instance.
   */
  public AlluxioCatalog() {
    mFileSystem = FileSystem.Factory.create(FileSystemContext.create(ServerConfiguration.global()));
    mUdbRegistry = new UnderDatabaseRegistry();
    mUdbRegistry.refresh();
    mLayoutRegistry = new LayoutRegistry();
    mLayoutRegistry.refresh();
  }

  /**
   * @return the layout registry
   */
  public LayoutRegistry getLayoutRegistry() {
    return mLayoutRegistry;
  }

  /**
   * Attaches an udb database to Alluxio catalog.
   *
   * @param journalContext journal context
   * @param udbType the database type
   * @param udbConnectionUri the udb connection uri
   * @param udbDbName the database name in the udb
   * @param dbName the database name in Alluxio
   * @param map the configuration
   * @return true if database successfully created
   */
  public boolean attachDatabase(JournalContext journalContext, String udbType,
      String udbConnectionUri, String udbDbName, String dbName, Map<String, String> map)
      throws IOException {
    if (mDBs.containsKey(dbName)) {
      throw new IOException(String
          .format("Unable to attach database. Database name %s (type: %s) already exists.",
              dbName, udbType));
    }

    applyAndJournal(journalContext, Journal.JournalEntry.newBuilder().setAttachDb(
        alluxio.proto.journal.Table.AttachDbEntry.newBuilder()
            .setUdbType(udbType)
            .setUdbConnectionUri(udbConnectionUri)
            .setUdbDbName(udbDbName)
            .setDbName(dbName)
            .putAllConfig(map).build()).build());

    try {
      mDBs.get(dbName).sync(journalContext);
    } catch (Exception e) {
      // Failed to connect to and sync the udb.
      applyAndJournal(journalContext, Journal.JournalEntry.newBuilder().setDetachDb(
          alluxio.proto.journal.Table.DetachDbEntry.newBuilder().setDbName(dbName).build())
          .build());
      throw new IOException(String
          .format("Failed to connect underDb for Alluxio db '%s': %s", dbName,
              e.getMessage()), e);
    }
    return true;
  }

  /**
   * Removes an existing database.
   *
   * @param journalContext journal context
   * @param dbName the database name
   * @return true if database successfully created
   */
  public boolean detachDatabase(JournalContext journalContext, String dbName)
      throws IOException {
    if (!mDBs.containsKey(dbName)) {
      throw new IOException(String
          .format("Unable to detach database. Database name %s does not exist", dbName));
    }
    applyAndJournal(journalContext, Journal.JournalEntry.newBuilder().setDetachDb(
        alluxio.proto.journal.Table.DetachDbEntry.newBuilder().setDbName(dbName).build()).build());
    return true;
  }

  /**
   * Get a table object by name.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @return a table object
   */
  public Table getTable(String dbName, String tableName) throws IOException {
    Database db = getDatabaseByName(dbName);
    return db.getTable(tableName);
  }

  /**
   * Get all databases.
   *
   * @return a list of all database names
   */
  public List<String> getAllDatabases() throws IOException {
    // TODO(gpang): update api to return collection or iterator?
    return new ArrayList<>(mDBs.keySet());
  }

  private Database getDatabaseByName(String dbName) throws NotFoundException {
    Database db = mDBs.get(dbName);
    if (db == null) {
      throw new NotFoundException("Database " + dbName + " does not exist");
    }
    return db;
  }

  /**
   * Get a list of tables in a database.
   *
   * @param dbName database name
   * @return a list of table names in the database
   */
  public List<String> getAllTables(String dbName) throws IOException {
    Database db = getDatabaseByName(dbName);
    return db.getTables().stream().map(Table::getName).collect(Collectors.toList());
  }

  /**
   * Returns the statistics for the specified table.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param colNames column names
   * @return the statistics for the specified table
   */
  public List<ColumnStatisticsInfo> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames)
      throws IOException {
    Table table = getTable(dbName, tableName);
    return table.getStatistics().stream()
        .filter(info -> colNames.contains(info.getColName())).collect(Collectors.toList());
  }

  /**
   * Returns the statistics for the specified table.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param partNames partition names
   * @param colNames column names
   * @return the statistics for the partitions for a specific table
   */
  public Map<String, ColumnStatisticsList> getPartitionColumnStatistics(String dbName,
      String tableName, List<String> partNames, List<String> colNames) throws IOException {
    Table table = getTable(dbName, tableName);
    List<Partition> partitions = table.getPartitions();
    return partitions.stream().filter(p -> partNames.contains(p.getBaseLayout().getSpec()))
        .map(p -> new Pair<>(p.getBaseLayout().getSpec(),
            ColumnStatisticsList.newBuilder().addAllStatistics(
                p.getBaseLayout().getColumnStatsData().entrySet().stream()
                    .filter(entry -> colNames.contains(entry.getKey()))
                    .map(Map.Entry::getValue).collect(Collectors.toList())).build()))
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, (e1, e2) -> e2));
  }

  /**
   * Returns the partitions based on a constraint for the specified table.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param constraint the column contraint
   * @return the partition info for the specified table
   */
  public List<alluxio.grpc.table.Partition> readTable(String dbName, String tableName,
      Constraint constraint) throws IOException {
    Table table = getTable(dbName, tableName);
    // TODO(david): implement partition pruning
    return table.getPartitions().stream().map(Partition::toProto).collect(Collectors.toList());
  }

  private void apply(alluxio.proto.journal.Table.AttachDbEntry entry) {
    String udbType = entry.getUdbType();
    String udbConnectionUri = entry.getUdbConnectionUri();
    String udbDbName = entry.getUdbDbName();
    String dbName = entry.getDbName();

    CatalogContext catalogContext = new CatalogContext(mUdbRegistry, mLayoutRegistry);
    UdbContext udbContext =
        new UdbContext(mUdbRegistry, mFileSystem, udbType, udbConnectionUri, udbDbName, dbName);

    Database db =
        Database.create(catalogContext, udbContext, udbType, dbName, entry.getConfigMap());
    mDBs.put(dbName, db);
  }

  private void apply(alluxio.proto.journal.Table.DetachDbEntry entry) {
    String dbName = entry.getDbName();
    mDBs.remove(dbName);
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    if (entry.hasAttachDb()) {
      apply(entry.getAttachDb());
      return true;
    } else if (entry.hasAddTable()) {
      Database db = mDBs.get(entry.getAddTable().getDbName());
      return db.processJournalEntry(entry);
    } else if (entry.hasDetachDb()) {
      apply(entry.getDetachDb());
      return true;
    }
    return false;
  }

  @Override
  public void resetState() {
    mDBs.clear();
  }

  private Iterator<Journal.JournalEntry> getDbIterator() {
    final Iterator<Map.Entry<String, Database>> it = mDBs.entrySet().iterator();
    return new Iterator<Journal.JournalEntry>() {
      private Map.Entry<String, Database> mEntry = null;

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
        String dbName = mEntry.getKey();
        Database database = mEntry.getValue();
        UdbContext udbContext = database.getUdb().getUdbContext();
        mEntry = null;

        return Journal.JournalEntry.newBuilder().setAttachDb(
            alluxio.proto.journal.Table.AttachDbEntry.newBuilder()
                .setUdbType(database.getType())
                .setUdbConnectionUri(udbContext.getConnectionUri())
                .setUdbDbName(udbContext.getUdbDbName())
                .setDbName(dbName)
                .putAllConfig(database.getConfig()).build()).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "GetDbIteratorr#Iterator#remove is not supported.");
      }
    };
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    List<Iterator<Journal.JournalEntry>> componentIters = StreamUtils
        .map(JournalEntryIterable::getJournalEntryIterator, mDBs.values());
    return Iterators.concat(getDbIterator(), Iterators.concat(componentIters.iterator()));
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.TABLE_MASTER_CATALOG;
  }
}
