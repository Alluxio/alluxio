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
import alluxio.grpc.CatalogMasterClientServiceGrpc;
import alluxio.grpc.GetAllDatabasesPRequest;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.List;

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
  public List<String> getAllDatabase() throws IOException {
    return retryRPC(() -> mClient.getAllDatabases(
        GetAllDatabasesPRequest.newBuilder().build()).getDatabaseList());
  }
}
