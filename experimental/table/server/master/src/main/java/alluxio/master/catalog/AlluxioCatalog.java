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

import alluxio.ProcessUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.ColumnStatisticsList;
import alluxio.grpc.catalog.Constraint;
import alluxio.grpc.catalog.Domain;
import alluxio.grpc.catalog.FieldSchema;
import alluxio.grpc.catalog.PartitionInfo;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Catalog;
import alluxio.proto.journal.Journal;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UnderDatabaseRegistry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

// TODO(yuzhu): journal the state of the catalog
/**
 * Class representing an Alluxio catalog service.
 */
public class AlluxioCatalog implements Journaled {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioCatalog.class);

  private final Map<String, Database> mDBs = new ConcurrentHashMap<>();
  private final UnderDatabaseRegistry mUdbRegistry;
  private final FileSystem mFileSystem;

  /**
   * Creates an instance.
   */
  public AlluxioCatalog() {
    mFileSystem = FileSystem.Factory.create(FileSystemContext.create(ServerConfiguration.global()));
    mUdbRegistry = new UnderDatabaseRegistry();
    mUdbRegistry.refresh();
  }

  /**
   * Attaches an existing database.
   *
   *
   * @param journalContext journal context
   * @param type the database type
   * @param dbName the database name
   * @param map the configuration
   * @return true if database successfully created
   */
  public boolean attachDatabase(JournalContext journalContext, String type,
      String dbName, Map<String, String> map)
      throws IOException {
    if (mDBs.containsKey(dbName)) {
      throw new IOException(String
          .format("Unable to attach database. Database name %s (type: %s) already exists.", dbName,
              type));
    }
    applyAndJournal(journalContext,
        Catalog.AttachDbEntry.newBuilder().setType(type).setDbName(dbName)
            .putAllConfig(map).build());
    mDBs.get(dbName).sync();
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
    Table table = db.getTable(tableName);
    return table;
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
    return partitions.stream().filter(p -> partNames.contains(p.getLayout().getSpec()))
        .map(p -> new Pair<>(p.getLayout().getSpec(),
            ColumnStatisticsList.newBuilder().addAllStatistics(
                p.getLayout().getColumnStatsData().entrySet().stream()
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
  public List<alluxio.grpc.catalog.Partition> readTable(String dbName, String tableName,
      Constraint constraint) throws IOException {
    Table table = getTable(dbName, tableName);
    List<Partition> partitions = table.getPartitions();

    // TODO(david): implement partition pruning

    return partitions.stream().map(Partition::toProto).collect(Collectors.toList());
  }

  private static boolean checkDomain(String value, FieldSchema schema, Domain constraint) {
    Comparable object;
    // TODO(yuzhu): handle more complex data types
    switch (schema.getType()) {
      case "boolean":
        object = Boolean.valueOf(value);
        break;
      case "int":
      case "integer":
        object = Integer.valueOf(value);
        break;
      case "long":
        object = Long.valueOf(value);
        break;
      case "string":
        object = value;
        break;
      case "double":
        object = Double.valueOf(value);
        break;
      default:
        return false;
    }
    return alluxio.master.catalog.Domain.parseFrom(constraint).isInDomain(object);
  }

  private static boolean checkDomain(PartitionInfo partitionInfo,
      Map<FieldSchema, Domain> constraints) {
    Preconditions.checkArgument(constraints.size() == partitionInfo.getValuesList().size(),
        "partition key size is not the same as constraint size");
    int index = 0;
    for (Map.Entry<FieldSchema, Domain> entry : constraints.entrySet()) {
      String val = partitionInfo.getValuesList().get(index++);
      if (!checkDomain(val, entry.getKey(), entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Apply AttachDB journal entry.
   *
   * @param context journal context
   * @param entry attachDb entry
   */
  public void applyAndJournal(Supplier<JournalContext> context,
      Catalog.AttachDbEntry entry) {
    LOG.info("Apply attachDb {} with type {} and configuration {}",
        entry.getDbName(), entry.getType(), entry.getConfigMap());
    try {
      apply(entry);
      context.get().append(Journal.JournalEntry.newBuilder().setAttachDb(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  private void apply(Catalog.AttachDbEntry entry) {
    String type = entry.getType();
    String dbName = entry.getDbName();

    UdbContext context = new UdbContext(mUdbRegistry, mFileSystem, type, dbName);

    CatalogConfiguration configuration = new CatalogConfiguration(entry.getConfigMap());
    Database db = Database.create(context, type, dbName, configuration);
    mDBs.put(dbName, db);
  }

  private void apply(Catalog.AddTableEntry entry) {
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    if (entry.hasAttachDb()) {
      apply(entry.getAttachDb());
      return true;
    } else if (entry.hasAddTable()) {
      apply(entry.getAddTable());
      return true;
    }
    return false;
  }

  @Override
  public void resetState() {
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    return null;
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.CATALOG_SERVICE_MASTER;
  }
}
