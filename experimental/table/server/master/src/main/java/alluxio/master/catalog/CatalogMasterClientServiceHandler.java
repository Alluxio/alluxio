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
import alluxio.experimental.ProtoUtils;
import alluxio.grpc.CatalogMasterClientServiceGrpc;

import alluxio.grpc.CreateDatabasePRequest;
import alluxio.grpc.CreateDatabasePResponse;
import alluxio.grpc.CreateTablePRequest;
import alluxio.grpc.CreateTablePResponse;
import alluxio.grpc.GetAllDatabasesPRequest;
import alluxio.grpc.GetAllDatabasesPResponse;

import alluxio.grpc.GetAllTablesPRequest;
import alluxio.grpc.GetAllTablesPResponse;
import alluxio.grpc.GetTablePRequest;
import alluxio.grpc.GetTablePResponse;
import alluxio.grpc.TableInfo;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.apache.iceberg.Table;
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
    RpcUtils.call(LOG, () -> GetTablePResponse.newBuilder()
            .setTableInfo(mCatalogMaster.getTable(request.getDbName(), request.getTableName()))
            .build(), "getTable", "", responseObserver);
  }

  @Override
  public void createTable(CreateTablePRequest request,
      StreamObserver<CreateTablePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Table table = mCatalogMaster.createTable(request.getDbName(), request.getTableName()
          , request.getSchema());
      TableInfo info;
      if (table != null) {
        info = TableInfo.newBuilder().setDbName(request.getDbName()).setTableName(request.getTableName())
            .setBaseLocation(table.location()).setSchema(ProtoUtils.toProto(table.schema())).build();
        return CreateTablePResponse.newBuilder()
            .setTableInfo(info)
            .setSuccess(true).build();
      }
      return CreateTablePResponse.newBuilder()
          .setSuccess(false).build();

    }, "createTable", "", responseObserver);
  }

  @Override
  public void createDatabase(CreateDatabasePRequest request,
      StreamObserver<CreateDatabasePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> CreateDatabasePResponse.newBuilder()
        .setSuccess(mCatalogMaster.createDatabase(request.getDbName())).build(), "createDatabase", "", responseObserver);
  }
}
