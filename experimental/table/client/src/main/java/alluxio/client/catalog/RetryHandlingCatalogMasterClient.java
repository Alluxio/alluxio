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
import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.experimental.ProtoUtils;
import alluxio.grpc.ServiceType;
import alluxio.grpc.catalog.AttachDatabasePRequest;
import alluxio.grpc.catalog.CatalogMasterClientServiceGrpc;
import alluxio.grpc.catalog.ColumnStatisticsInfo;
import alluxio.grpc.catalog.Constraint;
import alluxio.grpc.catalog.CreateDatabasePRequest;
import alluxio.grpc.catalog.CreateTablePRequest;
import alluxio.grpc.catalog.Database;
import alluxio.grpc.catalog.GetAllDatabasesPRequest;
import alluxio.grpc.catalog.GetAllTablesPRequest;
import alluxio.grpc.catalog.GetDatabasePRequest;
import alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest;
import alluxio.grpc.catalog.GetTableColumnStatisticsPRequest;
import alluxio.grpc.catalog.GetTablePRequest;
import alluxio.grpc.catalog.Partition;
import alluxio.grpc.catalog.PartitionInfo;
import alluxio.grpc.catalog.ReadTablePRequest;
import alluxio.grpc.catalog.TableInfo;
import alluxio.master.MasterClientContext;

import org.apache.iceberg.Schema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

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
    return ServiceType.CATALOG_MASTER_CLIENT_SERVICE;
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
  public List<String> getAllDatabases() throws AlluxioStatusException {
    return retryRPC(() -> mClient.getAllDatabases(
        GetAllDatabasesPRequest.newBuilder().build()).getDatabaseList());
  }

  @Override
  public Database getDatabase(String databaseName) throws AlluxioStatusException {
    return retryRPC(() -> mClient.getDatabase(GetDatabasePRequest.newBuilder()
        .setDbName(databaseName).build())).getDb();
  }

  @Override
  public List<String> getAllTables(String databaseName) throws AlluxioStatusException {
    return retryRPC(() -> mClient.getAllTables(
        GetAllTablesPRequest.newBuilder().setDatabase(databaseName).build()).getTableList());
  }

  @Override
  public TableInfo getTable(String databaseName, String tableName) throws AlluxioStatusException {
    return retryRPC(() -> mClient.getTable(
        GetTablePRequest.newBuilder().setDbName(databaseName).setTableName(tableName).build())
        .getTableInfo());
  }

  @Override
  public boolean attachDatabase(String dbName, String dbType, Map<String, String> configuration)
      throws AlluxioStatusException {
    return retryRPC(() -> mClient.attachDatabase(
        AttachDatabasePRequest.newBuilder().setDbName(dbName).setDbType(dbType)
            .putAllOptions(configuration).build()).getSuccess());
  }

  @Override
  public boolean createDatabase(String dbName, Map<String, String> configuration)
      throws AlluxioStatusException {
    return retryRPC(() -> mClient.createDatabase(
        CreateDatabasePRequest.newBuilder().setDbName(dbName).putAllOptions(configuration).build())
        .getSuccess());
  }

  @Override
  public TableInfo createTable(String dbName, String tableName, Schema schema)
      throws AlluxioStatusException {
    return retryRPC(() -> mClient.createTable(
        CreateTablePRequest.newBuilder().setDbName(dbName)
            .setTableName(tableName)
            .setSchema(ProtoUtils.toProto(schema)).build()).getTableInfo());
  }

  @Override
  public List<Partition> readTable(String databaseName, String tableName, Constraint constraint)
      throws AlluxioStatusException {
    return retryRPC(() -> mClient.readTable(
        ReadTablePRequest.newBuilder().setDbName(databaseName).setTableName(tableName)
            .setConstraint(constraint).build()).getPartitionsList());
  }

  @Override
  public List<ColumnStatisticsInfo> getTableColumnStatistics(
          String databaseName,
          String tableName,
          List<String> columnNames) throws AlluxioStatusException {
    return retryRPC(() -> mClient.getTableColumnStatistics(
        GetTableColumnStatisticsPRequest.newBuilder().setDbName(databaseName)
            .setTableName(tableName).addAllColNames(columnNames).build()).getStatisticsList());
  }

  @Override
  public List<String> getPartitionNames(
          String databaseName,
          String tableName) throws AlluxioStatusException {
    return null;
  }

  @Override
  public List<PartitionInfo> getPartitionsByNames(
          String databaseName,
          String tableName,
          List<String> partitionNames) throws AlluxioStatusException {
    return null;
  }

  @Override
  public PartitionInfo getPartition(
          String databaseName,
          String tableName,
          List<String> partitionValues) throws AlluxioStatusException {
    return null;
  }

  @Override
  public Map<String, List<ColumnStatisticsInfo>> getPartitionColumnStatistics(
          String databaseName,
          String tableName,
          List<String> partitionNames,
          List<String> columnNames) throws AlluxioStatusException {
    return retryRPC(() -> mClient.getPartitionColumnStatistics(
        GetPartitionColumnStatisticsPRequest.newBuilder().setDbName(databaseName)
            .setTableName(tableName).addAllColNames(columnNames)
            .addAllPartNames(partitionNames).build()).getPartitionStatisticsMap())
        .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            e->e.getValue().getStatisticsList(), (e1, e2) -> e1));
  }
}
