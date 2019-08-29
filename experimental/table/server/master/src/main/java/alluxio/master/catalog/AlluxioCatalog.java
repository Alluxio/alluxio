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

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.FileStatistics;
import alluxio.table.common.UdbContext;
import alluxio.table.common.UnderDatabaseRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

// TODO(yuzhu): journal the state of the catalog
/**
 * Class representing an Alluxio catalog service.
 */
public class AlluxioCatalog {
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
   * create a database.
   *
   * @param type the database type
   * @param dbName the database name
   * @param options the options
   * @return true if database successfully created
   */
  public boolean createDatabase(String type, String dbName, Map<String, String> options)
      throws IOException {
    Database db = Database
        .create(new UdbContext(mUdbRegistry, mFileSystem, type, dbName), type, dbName, options);
    if (mDBs.putIfAbsent(dbName, db) != null) {
      return false;
    }
    db.sync();
    return true;
  }

  /**
   * Creates a table.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param schema the table schema
   * @return the {@link Table} of the newly created table
   */
  public Table createTable(String dbName, String tableName, alluxio.grpc.Schema schema)
      throws IOException {
    Database db = mDBs.get(dbName);
    if (db == null) {
      throw new IOException("Database name does not exist: " + dbName);
    }
    return db.createTable(tableName, schema);
  }

  /**
   * Get a table object by name.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @return a table object
   */
  public Table getTable(String dbName, String tableName) throws IOException {
    Database db = mDBs.get(dbName);
    if (db == null) {
      throw new IOException("Database name does not exist: " + dbName);
    }
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

  /**
   * Get a list of tables in a database.
   *
   * @param dbName database name
   * @return a list of table names in the database
   */
  public List<String> getAllTables(String dbName) throws IOException {
    Database db = mDBs.get(dbName);
    return db.getTables().stream().map(Table::getName).collect(Collectors.toList());
  }

  /**
   * Returns the statistics for the specified table.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @return the statistics for the specified table
   */
  public Map<String, FileStatistics> getStatistics(String dbName, String tableName)
      throws IOException {
    Table table = getTable(dbName, tableName);
    return table.getStatistics();
  }
}
