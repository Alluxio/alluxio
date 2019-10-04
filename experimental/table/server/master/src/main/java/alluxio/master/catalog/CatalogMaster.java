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

import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.ColumnStatisticsList;
import alluxio.grpc.catalog.Constraint;
import alluxio.grpc.catalog.Partition;
import alluxio.grpc.catalog.Schema;
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
   * @param dbType the database type
   * @param configuration the configuration
   * @return true if creation is successful
   */
  boolean attachDatabase(String dbName, String dbType, CatalogConfiguration configuration)
      throws IOException;

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
   * @param colNames column names
   * @return a list of column statistics info
   */
  List<ColumnStatisticsInfo> getTableColumnStatistics(String databaseName, String tableName,
      List<String> colNames) throws IOException;

  /**
   * Returns metadata for reading a table given constraints.
   *
   * @param dbName database name
   * @param tableName table name
   * @param constraint constraint
   * @return a list of partition information
   */
  List<Partition> readTable(String dbName, String tableName, Constraint constraint)
      throws IOException;

  /**
   * Get statistics on the partitions.
   *
   * @param dbName database name
   * @param tableName table name
   * @param partNamesList partition names
   * @param colNamesList column names
   * @return a map mapping partition names to a list of column statistics info
   */
  Map<String, ColumnStatisticsList> getPartitionColumnStatistics(String dbName,
      String tableName, List<String> partNamesList, List<String> colNamesList)
    throws IOException;
}
