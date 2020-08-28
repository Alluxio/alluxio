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

import alluxio.RpcUtils;
import alluxio.grpc.table.AttachDatabasePRequest;
import alluxio.grpc.table.AttachDatabasePResponse;
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
import alluxio.grpc.table.ReadTablePRequest;
import alluxio.grpc.table.ReadTablePResponse;
import alluxio.grpc.table.SyncDatabasePRequest;
import alluxio.grpc.table.SyncDatabasePResponse;
import alluxio.grpc.table.SyncStatus;
import alluxio.grpc.table.TableMasterClientServiceGrpc;
import alluxio.grpc.table.TransformTablePRequest;
import alluxio.grpc.table.TransformTablePResponse;
import alluxio.master.table.transform.TransformJobInfo;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * This class is a gRPC handler for table master RPCs.
 */
public class TableMasterClientServiceHandler
    extends TableMasterClientServiceGrpc.TableMasterClientServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(TableMasterClientServiceHandler.class);

  private final TableMaster mTableMaster;

  /**
   * Creates a new instance of {@link TableMasterClientServiceHandler}.
   *
   * @param tableMaster the {@link TableMaster} the handler uses internally
   */
  public TableMasterClientServiceHandler(TableMaster tableMaster) {
    Preconditions.checkNotNull(tableMaster, "tableMaster");
    mTableMaster = tableMaster;
  }

  @Override
  public void attachDatabase(AttachDatabasePRequest request,
      StreamObserver<AttachDatabasePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      SyncStatus status = mTableMaster
          .attachDatabase(request.getUdbType(), request.getUdbConnectionUri(),
              request.getUdbDbName(), request.getDbName(), request.getOptionsMap(),
              request.getIgnoreSyncErrors());
      return AttachDatabasePResponse.newBuilder().setSuccess(status.getTablesErrorsCount() == 0)
          .setSyncStatus(status).build();
    }, "attachDatabase", "", responseObserver);
  }

  @Override
  public void detachDatabase(DetachDatabasePRequest request,
      StreamObserver<DetachDatabasePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> DetachDatabasePResponse.newBuilder().setSuccess(mTableMaster
            .detachDatabase(request.getDbName())).build(), "detachDatabase", "",
        responseObserver);
  }

  @Override
  public void getAllDatabases(GetAllDatabasesPRequest request,
      StreamObserver<GetAllDatabasesPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetAllDatabasesPResponse.newBuilder()
        .addAllDatabase(mTableMaster.getAllDatabases()).build(),
        "getAllDatabases", "", responseObserver);
  }

  @Override
  public void getAllTables(GetAllTablesPRequest request,
      StreamObserver<GetAllTablesPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetAllTablesPResponse.newBuilder()
        .addAllTable(mTableMaster.getAllTables(request.getDatabase())).build(),
        "getAllTables", "", responseObserver);
  }

  @Override
  public void getDatabase(GetDatabasePRequest request,
      StreamObserver<GetDatabasePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetDatabasePResponse.newBuilder().setDb(
        mTableMaster.getDatabase(request.getDbName())).build(),
        "getDatabase", "", responseObserver);
  }

  @Override
  public void getTable(GetTablePRequest request,
      StreamObserver<GetTablePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Table table = mTableMaster.getTable(request.getDbName(), request.getTableName());
      if (table != null) {
        return GetTablePResponse.newBuilder().setTableInfo(table.toProto()).build();
      }
      return GetTablePResponse.getDefaultInstance();
    }, "getTable", "", responseObserver);
  }

  @Override
  public void getTableColumnStatistics(GetTableColumnStatisticsPRequest request,
      StreamObserver<GetTableColumnStatisticsPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetTableColumnStatisticsPResponse.newBuilder().addAllStatistics(
        mTableMaster.getTableColumnStatistics(request.getDbName(),
            request.getTableName(), request.getColNamesList())).build(),
        "getTableColumnStatistics", "", responseObserver);
  }

  @Override
  public void getPartitionColumnStatistics(GetPartitionColumnStatisticsPRequest request,
      StreamObserver<GetPartitionColumnStatisticsPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetPartitionColumnStatisticsPResponse.newBuilder()
            .putAllPartitionStatistics(mTableMaster.getPartitionColumnStatistics(
                request.getDbName(), request.getTableName(), request.getPartNamesList(),
                request.getColNamesList())).build(),
        "getPartitionColumnStatistics", "", responseObserver);
  }

  @Override
  public void readTable(ReadTablePRequest request,
      StreamObserver<ReadTablePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> ReadTablePResponse.newBuilder().addAllPartitions(mTableMaster
        .readTable(request.getDbName(), request.getTableName(), request.getConstraint()))
        .build(), "readTable", "", responseObserver);
  }

  @Override
  public void transformTable(TransformTablePRequest request,
      StreamObserver<TransformTablePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> TransformTablePResponse.newBuilder().setJobId(mTableMaster
        .transformTable(request.getDbName(), request.getTableName(), request.getDefinition()))
        .build(), "transformTable", "", responseObserver);
  }

  @Override
  public void syncDatabase(SyncDatabasePRequest request,
      StreamObserver<SyncDatabasePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      SyncStatus status = mTableMaster.syncDatabase(request.getDbName());
      return SyncDatabasePResponse.newBuilder().setSuccess(status.getTablesErrorsCount() == 0)
          .setStatus(status).build();
    }, "syncDatabase", "", responseObserver);
  }

  @Override
  public void getTransformJobInfo(GetTransformJobInfoPRequest request,
      StreamObserver<GetTransformJobInfoPResponse> responseObserver) {
    if (request.hasJobId()) {
      RpcUtils.call(LOG, () -> GetTransformJobInfoPResponse.newBuilder().addInfo(mTableMaster
          .getTransformJobInfo(request.getJobId()).toProto()).build(),
          "getTransformJobInfo", "", responseObserver);
    } else {
      RpcUtils.call(LOG, () -> GetTransformJobInfoPResponse.newBuilder().addAllInfo(mTableMaster
              .getAllTransformJobInfo().stream().map(TransformJobInfo::toProto)
              .collect(Collectors.toList())).build(),
          "getTransformJobInfo", "", responseObserver);
    }
  }
}
