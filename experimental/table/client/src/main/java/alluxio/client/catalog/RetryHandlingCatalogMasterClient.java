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

import alluxio.AbstractMasterClient;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.experimental.Constants;
import alluxio.grpc.CatalogMasterClientServiceGrpc;
import alluxio.grpc.GetAllDatabasesPRequest;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;

/**
 * A wrapper for the gRPC client to interact with the catalog master.
 */
@ThreadSafe
public final class RetryHandlingCatalogMasterClient extends AbstractMasterClient
    implements CatalogMasterClient {
  private CatalogMasterClientServiceGrpc.CatalogMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new block master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingCatalogMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.BLOCK_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.CATALOG_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.CATALOG_MSTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = CatalogMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public List<String> getAllDatabase() throws AlluxioStatusException {
    return retryRPC(() -> mClient.getAllDatabases(
        GetAllDatabasesPRequest.newBuilder().build()).getDatabaseList());
  }

  @Override
  public Database getDatabase(String databaseName) throws AlluxioStatusException {
    return null;
  }

  @Override
  public List<String> getAllTables(String databaseName) throws AlluxioStatusException {
    return null;
  }

  @Override
  public Table getTable(String databaseName, String tableName) throws AlluxioStatusException {
    return null;
  }

  @Override
  public void createDatabase(Database database) throws AlluxioStatusException {}

  @Override
  public void createTable(Table table) throws AlluxioStatusException {}

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(
          String databaseName,
          String tableName,
          List<String> columnNames) throws AlluxioStatusException {
    return null;
  }

  @Override
  public List<String> getPartitionNames(
          String databaseName,
          String tableName) throws AlluxioStatusException {
    return null;
  }

  @Override
  public List<Partition> getPartitionsByNames(
          String databaseName,
          String tableName,
          List<String> partitionNames) throws AlluxioStatusException {
    return null;
  }

  @Override
  public Partition getPartition(
          String databaseName,
          String tableName,
          List<String> partitionValues) throws AlluxioStatusException {
    return null;
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
          String databaseName,
          String tableName,
          List<String> partitionNames,
          List<String> columnNames) throws AlluxioStatusException {
    return null;
  }
}
