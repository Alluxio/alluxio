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

package alluxio.client.catalog;

import alluxio.Client;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.master.MasterClientContext;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Partition;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;

/**
 * A client to use for interacting with a catalog master.
 */
@ThreadSafe
public interface CatalogMasterClient extends Client {

  /**
   * Factory for {@link CatalogMasterClient}.
   */
  class Factory {

    private Factory() {
    } // prevent instantiation

    /**
     * Factory method for {@link CatalogMasterClient}.
     *
     * @param conf master client configuration
     * @return a new {@link CatalogMasterClient} instance
     */
    public static CatalogMasterClient create(MasterClientContext conf) {
      return new RetryHandlingCatalogMasterClient(conf);
    }
  }

  /**
   * Get a list of all database names.
   *
   * @return list of database names
   * @throws AlluxioStatusException
   */
  List<String> getAllDatabase() throws AlluxioStatusException;

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
  Table getTable(String databaseName, String tableName) throws AlluxioStatusException;

  /**
   * Create database with given schema.
   *
   * @param database database metadata
   * @throws AlluxioStatusException
   */
  void createDatabase(Database database) throws AlluxioStatusException;

  /**
   * Create table with given schema.
   *
   * @param table table metadata
   * @throws AlluxioStatusException
   */
  void createTable(Table table) throws AlluxioStatusException;

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
  List<ColumnStatisticsObj> getTableColumnStatistics(
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
   * get partition metadata with given database name, table name and partition names.
   *
   * @param databaseName database name
   * @param tableName table name
   * @param partitionNames partition names
   * @return list of partition metadata
   * @throws AlluxioStatusException
   */
  List<Partition> getPartitionsByNames(
          String databaseName,
          String tableName,
          List<String> partitionNames) throws AlluxioStatusException;

  /**
   * get partition metadata with given database name, table name and partition values.
   *
   * @param databaseName database name
   * @param tableName table name
   * @param partitionValues partition values
   * @return partition metadata
   * @throws AlluxioStatusException
   */
  Partition getPartition(
          String databaseName,
          String tableName,
          List<String> partitionValues) throws AlluxioStatusException;

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
  Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
          String databaseName,
          String tableName,
          List<String> partitionNames,
          List<String> columnNames) throws AlluxioStatusException;
}
