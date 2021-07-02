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

import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.ColumnStatisticsList;
import alluxio.grpc.table.Constraint;
import alluxio.grpc.table.Database;
import alluxio.grpc.table.Partition;
import alluxio.grpc.table.SyncStatus;
import alluxio.master.Master;
import alluxio.master.table.transform.TransformJobInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface of the table master that manages the table service.
 */
public interface TableMaster extends Master {

  /**
   * Attach an existing database to the catalog.
   *
   * @param udbType the database type
   * @param udbConnectionUri the udb connection uri
   * @param udbDbName the database name in the udb
   * @param dbName the database name in Alluxio
   * @param configuration the configuration
   * @param ignoreSyncErrors if true, will ignore sync errors during the attach
   * @return the sync status for the attach
   */
  SyncStatus attachDatabase(String udbType, String udbConnectionUri, String udbDbName,
      String dbName, Map<String, String> configuration, boolean ignoreSyncErrors)
      throws IOException;

  /**
   * Remove an existing database in the catalog.
   *
   * @param dbName the database name to remove
   * @return true if deletion is successful
   */
  boolean detachDatabase(String dbName)
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
   * Gets a database object.
   *
   * @param dbName the database name
   * @return a database object
   */
  Database getDatabase(String dbName) throws IOException;

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

  /**
   * Transforms a table to a new table.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param definition the transformation definition
   * @return the job ID
   */
  long transformTable(String dbName, String tableName, String definition) throws IOException;

  /**
   * @param jobId the job ID
   * @return the information for the transformation job
   */
  TransformJobInfo getTransformJobInfo(long jobId) throws IOException;

  /**
   * @return a list of information for all the transformation jobs
   */
  List<TransformJobInfo> getAllTransformJobInfo() throws IOException;

  /**
   * Syncs a database.
   *
   * @param dbName the database name
   * @return the resulting sync status
   */
  SyncStatus syncDatabase(String dbName) throws IOException;
}
