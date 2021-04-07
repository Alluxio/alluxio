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
import alluxio.grpc.table.AttachDatabasePResponse;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Constraint;
import alluxio.grpc.table.Database;
import alluxio.grpc.table.DetachDatabasePRequest;
import alluxio.grpc.table.DetachDatabasePResponse;
import alluxio.grpc.table.GetAllDatabasesPRequest;
import alluxio.grpc.table.GetAllDatabasesPResponse;
import alluxio.grpc.table.GetAllTablesPRequest;
import alluxio.grpc.table.GetAllTablesPResponse;
import alluxio.grpc.table.GetDatabasePRequest;
import alluxio.grpc.table.GetDatabasePResponse;
import alluxio.grpc.table.GetPartitionColumnStatisticsPRequest;
import alluxio.grpc.table.GetPartitionColumnStatisticsPResponse;
import alluxio.grpc.table.GetTableColumnStatisticsPRequest;
import alluxio.grpc.table.GetTableColumnStatisticsPResponse;
import alluxio.grpc.table.GetTablePRequest;
import alluxio.grpc.table.GetTablePResponse;
import alluxio.grpc.table.GetTransformJobInfoPRequest;
import alluxio.grpc.table.GetTransformJobInfoPResponse;
import alluxio.grpc.table.Partition;
import alluxio.grpc.table.ReadTablePRequest;
import alluxio.grpc.table.ReadTablePResponse;
import alluxio.grpc.table.SyncDatabasePRequest;
import alluxio.grpc.table.SyncDatabasePResponse;
import alluxio.grpc.table.SyncStatus;
import alluxio.grpc.table.TableInfo;
import alluxio.grpc.table.TableMasterClientServiceGrpc;
import alluxio.grpc.table.TransformJobInfo;
import alluxio.grpc.table.TransformTablePRequest;
import alluxio.grpc.table.TransformTablePResponse;
import alluxio.master.MasterClientContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger RPC_LOG = LoggerFactory.getLogger(TableMasterClient.class);
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
    return retryRPC(() -> {
      GetAllDatabasesPResponse response = mClient.getAllDatabases(
          GetAllDatabasesPRequest.newBuilder().build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getAllDatabases response has {} bytes, {} databases",
            response.getSerializedSize(), response.getDatabaseCount());
      }
      return response.getDatabaseList();
    }, RPC_LOG, "GetAllDatabases", "");
  }

  @Override
  public Database getDatabase(String databaseName) throws AlluxioStatusException {
    return retryRPC(() -> {
      GetDatabasePResponse response = mClient.getDatabase(GetDatabasePRequest.newBuilder()
          .setDbName(databaseName).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getDatabase response has {} bytes", response.getSerializedSize());
      }
      return response;
    }, RPC_LOG, "GetDatabase", "databaseName=%s", databaseName).getDb();
  }

  @Override
  public List<String> getAllTables(String databaseName) throws AlluxioStatusException {
    return retryRPC(() -> {
      GetAllTablesPResponse response = mClient.getAllTables(
          GetAllTablesPRequest.newBuilder().setDatabase(databaseName).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getAllTables response has {} bytes, {} tables", response.getSerializedSize(),
                response.getTableCount());
      }
      return response.getTableList();
    }, RPC_LOG, "GetAllTables", "databaseName=%s", databaseName);
  }

  @Override
  public TableInfo getTable(String databaseName, String tableName) throws AlluxioStatusException {
    return retryRPC(() -> {
      GetTablePResponse response = mClient.getTable(
          GetTablePRequest.newBuilder().setDbName(databaseName).setTableName(tableName).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getTable response has {} bytes", response.getSerializedSize());
      }
      return response.getTableInfo();
    }, RPC_LOG, "GetTable", "databaseName=%s,tableName=%s",
        databaseName, tableName);
  }

  @Override
  public SyncStatus attachDatabase(String udbType, String udbConnectionUri, String udbDbName,
      String dbName, Map<String, String> configuration, boolean ignoreSyncErrors)
      throws AlluxioStatusException {
    return retryRPC(() -> {
      AttachDatabasePResponse response = mClient.attachDatabase(
          AttachDatabasePRequest.newBuilder().setUdbType(udbType)
              .setUdbConnectionUri(udbConnectionUri).setUdbDbName(udbDbName).setDbName(dbName)
              .putAllOptions(configuration).setIgnoreSyncErrors(ignoreSyncErrors).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("attachDatabase response has {} bytes", response.getSerializedSize());
      }
      return response.getSyncStatus();
    }, RPC_LOG, "AttachDatabase", "udbType=%s,udbConnectionUri=%s,"
            + "udbDbName=%s,dbName=%s, configuration=%s,ignoreSyncErrors=%s",
        udbType, udbConnectionUri, udbDbName, dbName, configuration, ignoreSyncErrors);
  }

  @Override
  public boolean detachDatabase(String dbName)
      throws AlluxioStatusException {
    return retryRPC(() -> {
      DetachDatabasePResponse response = mClient.detachDatabase(
          DetachDatabasePRequest.newBuilder().setDbName(dbName).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("detachDatabase response has {} bytes", response.getSerializedSize());
      }
      return response.getSuccess();
    }, RPC_LOG, "DetachDatabase", "dbName=%s", dbName);
  }

  @Override
  public SyncStatus syncDatabase(String dbName) throws AlluxioStatusException {
    return retryRPC(() -> {
      SyncDatabasePResponse response = mClient.syncDatabase(
          SyncDatabasePRequest.newBuilder().setDbName(dbName).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("syncDatabase response has {} bytes", response.getSerializedSize());
      }
      return response.getStatus();
    }, RPC_LOG, "SyncDatabase", "dbName=%s", dbName);
  }

  @Override
  public List<Partition> readTable(String databaseName, String tableName, Constraint constraint)
      throws AlluxioStatusException {
    return retryRPC(() -> {
      ReadTablePResponse response = mClient.readTable(
          ReadTablePRequest.newBuilder().setDbName(databaseName).setTableName(tableName)
              .setConstraint(constraint).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("readTable response has {} bytes, {} partitions",
            response.getSerializedSize(), response.getPartitionsCount());
      }
      return response.getPartitionsList();
    }, RPC_LOG, "ReadTable", "databaseName=%s,tableName=%s,constraint=%s",
        databaseName, tableName, constraint);
  }

  @Override
  public List<ColumnStatisticsInfo> getTableColumnStatistics(
          String databaseName,
          String tableName,
          List<String> columnNames) throws AlluxioStatusException {
    return retryRPC(() -> {
      GetTableColumnStatisticsPResponse response = mClient.getTableColumnStatistics(
          GetTableColumnStatisticsPRequest.newBuilder().setDbName(databaseName)
              .setTableName(tableName).addAllColNames(columnNames).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getTableColumnStatistics response has {} bytes, {} statistics",
            response.getSerializedSize(), response.getStatisticsCount());
      }
      return response.getStatisticsList();
    }, RPC_LOG, "GetTableColumnStatistics",
        "databaseName=%s,tableName=%s,columnNames=%s",
        databaseName, tableName, columnNames);
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
    return retryRPC(() -> {
      GetPartitionColumnStatisticsPResponse response = mClient.getPartitionColumnStatistics(
          GetPartitionColumnStatisticsPRequest.newBuilder().setDbName(databaseName)
              .setTableName(tableName).addAllColNames(columnNames)
              .addAllPartNames(partitionNames).build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getPartitionColumnStatistics response has {} bytes, {} partition statistics",
                response.getSerializedSize(),
                response.getPartitionStatisticsCount());
      }
      return response.getPartitionStatisticsMap();
    }, RPC_LOG, "GetPartitionColumnStatistics",
        "databaseName=%s,tableName=%s,partitionNames=%s,columnNames=%s",
        databaseName, tableName, partitionNames, columnNames)
        .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            e->e.getValue().getStatisticsList(), (e1, e2) -> e1));
  }

  @Override
  public long transformTable(String dbName, String tableName, String definition)
      throws AlluxioStatusException {
    return retryRPC(() -> {
      TransformTablePResponse response = mClient.transformTable(
          TransformTablePRequest.newBuilder()
              .setDbName(dbName)
              .setTableName(tableName)
              .setDefinition(definition)
              .build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("transformTable response has {} bytes",
                response.getSerializedSize());
      }
      return response.getJobId();
    }, RPC_LOG, "TransformTable", "dbName=%s,tableName=%s,definition=%s",
        dbName, tableName, definition);
  }

  @Override
  public TransformJobInfo getTransformJobInfo(long jobId) throws AlluxioStatusException {
    return retryRPC(() -> {
      GetTransformJobInfoPResponse response = mClient.getTransformJobInfo(
          GetTransformJobInfoPRequest.newBuilder()
              .setJobId(jobId)
              .build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getTransformJobInfo response has {} bytes, {} JobInfo",
                response.getSerializedSize(),
                response.getInfoCount());
      }
      return response.getInfo(0);
    }, RPC_LOG, "GetTransformJobInfo", "jobId=%d", jobId);
  }

  @Override
  public List<TransformJobInfo> getAllTransformJobInfo() throws AlluxioStatusException {
    return retryRPC(() -> {
      GetTransformJobInfoPResponse response = mClient.getTransformJobInfo(
          GetTransformJobInfoPRequest.newBuilder().build());
      if (RPC_LOG.isDebugEnabled()) {
        RPC_LOG.debug("getAllTransformJobInfo response has {} bytes, {} JobInfo",
                response.getSerializedSize(),
                response.getInfoCount());
      }
      return response.getInfoList();
    }, RPC_LOG, "GetAllTransformJobInfo", "");
  }
}
