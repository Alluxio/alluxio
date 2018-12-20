/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.meta;

import alluxio.RpcUtils;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.grpc.MetaMasterConfigurationServiceGrpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a gRPC handler for meta master RPCs.
 */
public final class MetaMasterConfigurationServiceHandler
    extends MetaMasterConfigurationServiceGrpc.MetaMasterConfigurationServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(MetaMasterConfigurationServiceHandler.class);

  private final MetaMaster mMetaMaster;

  /**
   * @param metaMaster the Alluxio meta master
   */
  public MetaMasterConfigurationServiceHandler(MetaMaster metaMaster) {
    mMetaMaster = metaMaster;
  }

  @Override
  public void getConfiguration(GetConfigurationPOptions options,
      StreamObserver<GetConfigurationPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetConfigurationPResponse>) () -> {
      return GetConfigurationPResponse.newBuilder()
          .addAllConfigs(mMetaMaster.getConfiguration(options)).build();
    }, "getConfiguration", "options=%s", responseObserver, options);
  }
}
