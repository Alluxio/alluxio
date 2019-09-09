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

import alluxio.grpc.Constraint;
import alluxio.grpc.FileStatistics;
import alluxio.grpc.PartitionInfo;
import alluxio.grpc.Schema;
import alluxio.master.Master;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface of the catalog master that manages the catalog metadata.
 */
public interface CatalogMaster extends Master {

  /**
   * Attach an existing database to the catalog.
   *
   * @param dbName the database name to attach to
   * @param configuration the configuration
   * @return true if creation is successful
   */
  boolean attachDatabase(String dbName, CatalogConfiguration configuration) throws IOException;

  /**
   * Get a listing of all databases.
   *
   * @return a list of database
   */
  List<String> getAllDatabases() throws IOException;

  /**
   * Get a listing of all tables in a database.
   *
   * @param databaseName database name
   *
   * @return a list of tables
   */
  List<String> getAllTables(String databaseName) throws IOException;

  /**
   * Create a database.
   *
   * @param dbName a database name
   * @param configuration the configuration
   * @return true if creation is successful
   */
  boolean createDatabase(String dbName, CatalogConfiguration configuration) throws IOException;

  /**
   * Create a table.
   *  @param dbName database name
   * @param tableName table name
   * @param schema schema
   * @return a Table object
   */
  Table createTable(String dbName, String tableName, Schema schema) throws IOException;

  /**
   * Get a table.
   *
   * @param databaseName database name
   * @param tableName table name
   *
   * @return a Table object
   */
  Table getTable(String databaseName, String tableName) throws IOException;

  /**
   * Get statistics on the table.
   *
   * @param databaseName database name
   * @param tableName table name
   *
   * @return a map containing data files paths mapped to file statistics
   */
  Map<String, FileStatistics> getStatistics(String databaseName, String tableName)
      throws IOException;

  /**
   * Get the list of datafiles on the table.
   *
   * @param databaseName database name
   * @param tableName table name
   *
   * @return a list containing the data files paths
   */
  List<String> getDataFiles(String databaseName, String tableName) throws IOException;


  /**
   * Get a map of partitions and related partition info based on constraints.
   *
   * @param dbName database name
   * @param tableName table name
   * @param constraint constraint
   *
   * @return a map of partitions and related partition info
   */
  List<PartitionInfo> getPartitions(String dbName, String tableName, Constraint constraint)
      throws IOException;
}
