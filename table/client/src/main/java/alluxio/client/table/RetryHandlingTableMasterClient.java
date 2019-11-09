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

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.ServiceType;
import alluxio.grpc.table.AttachDatabasePRequest;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Constraint;
import alluxio.grpc.table.Database;
import alluxio.grpc.table.DetachDatabasePRequest;
import alluxio.grpc.table.GetAllDatabasesPRequest;
import alluxio.grpc.table.GetAllTablesPRequest;
import alluxio.grpc.table.GetDatabasePRequest;
import alluxio.grpc.table.GetPartitionColumnStatisticsPRequest;
import alluxio.grpc.table.GetTableColumnStatisticsPRequest;
import alluxio.grpc.table.GetTablePRequest;
import alluxio.grpc.table.GetTransformJobInfoPRequest;
import alluxio.grpc.table.Partition;
import alluxio.grpc.table.ReadTablePRequest;
import alluxio.grpc.table.SyncDatabasePRequest;
import alluxio.grpc.table.TableInfo;
import alluxio.grpc.table.TableMasterClientServiceGrpc;
import alluxio.grpc.table.TransformJobInfo;
import alluxio.grpc.table.TransformTablePRequest;
import alluxio.master.MasterClientContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the table master.
 */
@ThreadSafe
public final class RetryHandlingTableMasterClient extends AbstractMasterClient
    implements TableMasterClient {
  private TableMasterClientServiceGrpc.TableMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new block master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingTableMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.TABLE_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.TABLE_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.TABLE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = TableMasterClientServiceGrpc.newBlockingStub(mChannel);
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
  public boolean attachDatabase(String udbType, String udbConnectionUri, String udbDbName,
      String dbName, Map<String, String> configuration) throws AlluxioStatusException {
    return retryRPC(() -> mClient.attachDatabase(
        AttachDatabasePRequest.newBuilder().setUdbType(udbType)
            .setUdbConnectionUri(udbConnectionUri).setUdbDbName(udbDbName).setDbName(dbName)
            .putAllOptions(configuration).build()).getSuccess());
  }

  @Override
  public boolean detachDatabase(String dbName)
      throws AlluxioStatusException {
    return retryRPC(() -> mClient.detachDatabase(
        DetachDatabasePRequest.newBuilder().setDbName(dbName).build()).getSuccess());
  }

  @Override
  public boolean syncDatabase(String dbName) throws AlluxioStatusException {
    return retryRPC(() -> mClient.syncDatabase(
        SyncDatabasePRequest.newBuilder().setDbName(dbName).build()).getSuccess());
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

  @Override
  public long transformTable(String dbName, String tableName, String definition)
      throws AlluxioStatusException {
    return retryRPC(() -> mClient.transformTable(
        TransformTablePRequest.newBuilder()
            .setDbName(dbName)
            .setTableName(tableName)
            .setDefinition(definition)
            .build()).getJobId());
  }

  @Override
  public TransformJobInfo getTransformJobInfo(long jobId) throws AlluxioStatusException {
    return retryRPC(() -> mClient.getTransformJobInfo(
        GetTransformJobInfoPRequest.newBuilder()
            .setJobId(jobId)
            .build()).getInfo(0));
  }

  @Override
  public List<TransformJobInfo> getAllTransformJobInfo() throws AlluxioStatusException {
    return retryRPC(() -> mClient.getTransformJobInfo(
        GetTransformJobInfoPRequest.newBuilder().build()).getInfoList());
  }
}
