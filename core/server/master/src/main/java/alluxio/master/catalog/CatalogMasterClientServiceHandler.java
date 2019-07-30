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
import alluxio.grpc.CatalogMasterClientServiceGrpc;

import alluxio.grpc.GetAllDatabasesPRequest;
import alluxio.grpc.GetAllDatabasesPResponse;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogMasterClientServiceHandler
    extends CatalogMasterClientServiceGrpc.CatalogMasterClientServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogMasterClientServiceHandler.class);

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
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetAllDatabasesPResponse>) () -> {

      return GetAllDatabasesPResponse.newBuilder()
          .addAllDatabase(mCatalogMaster.getAllDatabases()).build();
    }, "getAllDatabase", "", responseObserver);
  }
}
