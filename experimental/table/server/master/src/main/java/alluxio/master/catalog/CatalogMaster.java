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
import alluxio.master.Master;
//TODO(yuzhu): replace these classes with our own version of Database and Table classes

import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;

/**
 * Interface of the catalog master that manages the catalog metadata.
 */
public interface CatalogMaster extends Master {
  /**
   * Get a listing of all databases.
   *
   * @return a list of database
   */
  List<String> getAllDatabases();

  /**
   * Get a listing of all tables in a database.
   *
   * @param databaseName database name
   *
   * @return a list of tables
   */
  List<String> getAllTables(String databaseName);

  /**
   * Create a database.
   *
   * @param database a database name
   *
   */
  boolean createDatabase(String dbName);

  /**
   * Create a table.
   *  @param dbName database name
   * @param tableName table name
   * @param schema schema
   *
   */
  Table createTable(String dbName, String tableName, Schema schema);

  /**
   * Get a table.
   */
  Table getTable(String databaseName, String tableName);

  /**
   * Get statistics on the table
   */

  Map<String, FileStatistics> getStatistics(String databaseName, String tableName);

  List<String> getDataFiles(String dbName, String tableName);
}
