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

import alluxio.RpcUtils;
import alluxio.grpc.catalog.AttachDatabasePRequest;
import alluxio.grpc.catalog.AttachDatabasePResponse;
import alluxio.grpc.catalog.CatalogMasterClientServiceGrpc;
import alluxio.grpc.catalog.CreateDatabasePRequest;
import alluxio.grpc.catalog.CreateDatabasePResponse;
import alluxio.grpc.catalog.CreateTablePRequest;
import alluxio.grpc.catalog.CreateTablePResponse;
import alluxio.grpc.catalog.GetAllDatabasesPRequest;
import alluxio.grpc.catalog.GetAllDatabasesPResponse;
import alluxio.grpc.catalog.GetAllTablesPRequest;
import alluxio.grpc.catalog.GetAllTablesPResponse;
import alluxio.grpc.catalog.GetTableColumnStatisticsPRequest;
import alluxio.grpc.catalog.GetTableColumnStatisticsPResponse;
import alluxio.grpc.catalog.GetTablePRequest;
import alluxio.grpc.catalog.GetTablePResponse;
import alluxio.grpc.catalog.ReadTablePRequest;
import alluxio.grpc.catalog.ReadTablePResponse;
import alluxio.grpc.catalog.TableInfo;
import alluxio.grpc.catalog.TransformTablePRequest;
import alluxio.grpc.catalog.TransformTablePResponse;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a gRPC handler for catalog master RPCs.
 */
public class CatalogMasterClientServiceHandler
    extends CatalogMasterClientServiceGrpc.CatalogMasterClientServiceImplBase {
  private static final Logger LOG
      = LoggerFactory.getLogger(CatalogMasterClientServiceHandler.class);

  private final CatalogMaster mCatalogMaster;

  /**
   * Creates a new instance of {@link CatalogMasterClientServiceHandler}.
   *
   * @param catalogMaster the {@link CatalogMaster} the handler uses internally
   */
  public CatalogMasterClientServiceHandler(CatalogMaster catalogMaster) {
    Preconditions.checkNotNull(catalogMaster, "catalogMaster");
    mCatalogMaster = catalogMaster;
  }

  @Override
  public void attachDatabase(AttachDatabasePRequest request,
      StreamObserver<AttachDatabasePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> AttachDatabasePResponse.newBuilder().setSuccess(mCatalogMaster
            .attachDatabase(request.getDbName(), request.getDbType(),
                new CatalogConfiguration(request.getOptionsMap()))).build(), "attachDatabase", "",
        responseObserver);
  }

  @Override
  public void getAllDatabases(GetAllDatabasesPRequest request,
      StreamObserver<GetAllDatabasesPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetAllDatabasesPResponse.newBuilder()
        .addAllDatabase(mCatalogMaster.getAllDatabases()).build(),
        "getAllDatabases", "", responseObserver);
  }

  @Override
  public void getAllTables(GetAllTablesPRequest request,
      StreamObserver<GetAllTablesPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetAllTablesPResponse.newBuilder()
        .addAllTable(mCatalogMaster.getAllTables(request.getDatabase())).build(),
        "getAllTables", "", responseObserver);
  }

  @Override
  public void getTable(GetTablePRequest request,
      StreamObserver<GetTablePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Table table = mCatalogMaster.getTable(request.getDbName(), request.getTableName());
      if (table != null) {
        return GetTablePResponse.newBuilder().setTableInfo(table.toProto()).build();
      }
      return GetTablePResponse.getDefaultInstance();
    }, "getTable", "", responseObserver);
  }

  @Override
  public void createTable(CreateTablePRequest request,
      StreamObserver<CreateTablePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Table table = mCatalogMaster.createTable(request.getDbName(), request.getTableName(),
          request.getSchema());
      TableInfo info;
      if (table != null) {
        TableVersion tv = table.get();
        info = TableInfo.newBuilder().setDbName(request.getDbName())
            .setTableName(request.getTableName())
            .setBaseLocation(tv.getBaseLocation()).setSchema(tv.getSchema())
            .build();
        return CreateTablePResponse.newBuilder()
            .setTableInfo(info)
            .setSuccess(true).build();
      } else {
        return CreateTablePResponse.newBuilder()
            .setSuccess(false).build();
      }
    }, "createTable", "", responseObserver);
  }

  @Override
  public void createDatabase(CreateDatabasePRequest request,
      StreamObserver<CreateDatabasePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> CreateDatabasePResponse.newBuilder().setSuccess(mCatalogMaster
        .createDatabase(request.getDbName(), new CatalogConfiguration(request.getOptionsMap())))
        .build(), "createDatabase", "", responseObserver);
  }

  @Override
  public void getTableColumnStatistics(GetTableColumnStatisticsPRequest request,
      StreamObserver<GetTableColumnStatisticsPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetTableColumnStatisticsPResponse.newBuilder().addAllStatistics(
        mCatalogMaster.getTableColumnStatistics(request.getDbName(),
            request.getTableName(), request.getColNamesList())).build(),
        "getTableColumnStatistics", "", responseObserver);
  }

  @Override
  public void readTable(ReadTablePRequest request,
      StreamObserver<ReadTablePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> ReadTablePResponse.newBuilder().addAllPartitions(mCatalogMaster
        .readTable(request.getDbName(), request.getTableName(), request.getConstraint()))
        .build(), "readTable", "", responseObserver);
  }

  @Override
  public void transformTable(TransformTablePRequest request,
      StreamObserver<TransformTablePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mCatalogMaster.transformTable(request.getDbName(), request.getTableName(), request.getType(),
          request.getPartitionsMap());
      return TransformTablePResponse.getDefaultInstance();
    }, "transformTable", "", responseObserver);
  }
}
