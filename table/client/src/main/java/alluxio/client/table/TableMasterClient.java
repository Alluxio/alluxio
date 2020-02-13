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

package alluxio.client.table;

import alluxio.Client;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Constraint;
import alluxio.grpc.table.Database;
import alluxio.grpc.table.Partition;
import alluxio.grpc.table.SyncStatus;
import alluxio.grpc.table.TableInfo;
import alluxio.grpc.table.TransformJobInfo;
import alluxio.master.MasterClientContext;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A client to use for interacting with a table master.
 */
@ThreadSafe
public interface TableMasterClient extends Client {
  /**
   * Factory for {@link TableMasterClient}.
   */
  class Factory {

    private Factory() {
    } // prevent instantiation

    /**
     * Factory method for {@link TableMasterClient}.
     *
     * @param conf master client configuration
     * @return a new {@link TableMasterClient} instance
     */
    public static TableMasterClient create(MasterClientContext conf) {
      return new RetryHandlingTableMasterClient(conf);
    }
  }

  /**
   * Get a list of all database names.
   *
   * @return list of database names
   * @throws AlluxioStatusException
   */
  List<String> getAllDatabases() throws AlluxioStatusException;

  /**
   * Get database metadata.
   *
   * @param databaseName database name
   * @return database metadata
   */
  Database getDatabase(String databaseName) throws AlluxioStatusException;

  /**
   * Get a list of all table names.
   *
   * @param databaseName database name
   * @return list of table names
   * @throws AlluxioStatusException
   */
  List<String> getAllTables(String databaseName) throws AlluxioStatusException;

  /**
   * Get table metadata.
   *
   * @param databaseName database name
   * @param tableName table name
   * @return table metadata
   * @throws AlluxioStatusException
   */
  TableInfo getTable(String databaseName, String tableName) throws AlluxioStatusException;

  /**
   * Attaches an existing database.
   *
   * @param udbType the database type
   * @param udbConnectionUri the udb connection uri
   * @param udbDbName the database name in the udb
   * @param dbName the database name in Alluxio
   * @param configuration the configuration map
   * @param ignoreSyncErrors will ignore sync errors if true
   * @return the sync status for the attach
   * @throws AlluxioStatusException
   */
  SyncStatus attachDatabase(String udbType, String udbConnectionUri, String udbDbName,
      String dbName, Map<String, String> configuration, boolean ignoreSyncErrors)
      throws AlluxioStatusException;

  /**
   * Detaches an existing database in the catalog master.
   *
   * @param dbName database name
   * @return true if database created successfully
   * @throws AlluxioStatusException
   */
  boolean detachDatabase(String dbName)
      throws AlluxioStatusException;

  /**
   * Syncs an existing database in the catalog master.
   *
   * @param dbName database name
   * @return the sync status
   */
  SyncStatus syncDatabase(String dbName) throws AlluxioStatusException;

  /**
   * Returns metadata for reading a table given constraints.
   *
   * @param databaseName database name
   * @param tableName table name
   * @param constraint constraint on the columns
   * @return list of partitions
   * @throws AlluxioStatusException
   */
  List<Partition> readTable(String databaseName, String tableName, Constraint constraint)
      throws AlluxioStatusException;

  /**
   * Get table column statistics with given database name,
   * table name and list of column names.
   *
   * @param databaseName database name
   * @param tableName table name
   * @param columnNames column names
   * @return list of column statistics
   * @throws AlluxioStatusException
   */
  List<ColumnStatisticsInfo> getTableColumnStatistics(
          String databaseName,
          String tableName,
          List<String> columnNames) throws AlluxioStatusException;

  /**
   * Get partition names with given database name and table name.
   *
   * @param databaseName database name
   * @param tableName table name
   * @return list of partition names
   * @throws AlluxioStatusException
   */
  List<String> getPartitionNames(
          String databaseName,
          String tableName) throws AlluxioStatusException;

  /**
   * Get column statistics for selected partition and column.
   *
   * @param databaseName database name
   * @param tableName table name
   * @param partitionNames partition names
   * @param columnNames column names
   * @return Map<String partitionName, Map<String columnName, columnStatistics>>
   * @throws AlluxioStatusException
   */
  Map<String, List<ColumnStatisticsInfo>> getPartitionColumnStatistics(
          String databaseName,
          String tableName,
          List<String> partitionNames,
          List<String> columnNames) throws AlluxioStatusException;

  /**
   * Transforms a table.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param definition the transformation definition
   * @return job ID which can be used to poll the job status from job service
   * @throws AlluxioStatusException
   */
  long transformTable(String dbName, String tableName, String definition)
      throws AlluxioStatusException;

  /**
   * @param jobId the transformation job's ID
   * @return the job info
   */
  TransformJobInfo getTransformJobInfo(long jobId) throws AlluxioStatusException;

  /**
   * @return a list of information for all transformation jobs
   */
  List<TransformJobInfo> getAllTransformJobInfo() throws AlluxioStatusException;
}
