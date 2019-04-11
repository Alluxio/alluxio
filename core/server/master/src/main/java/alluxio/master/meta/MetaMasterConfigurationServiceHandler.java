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

package alluxio.master.meta;

import alluxio.RpcUtils;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.grpc.MetaMasterConfigurationServiceGrpc;
import alluxio.grpc.RemovePathConfigurationPRequest;
import alluxio.grpc.RemovePathConfigurationPResponse;
import alluxio.grpc.SetPathConfigurationPRequest;
import alluxio.grpc.SetPathConfigurationPResponse;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetConfigurationPResponse>) () ->
        GetConfigurationPResponse.newBuilder()
            .addAllConfigs(mMetaMaster.getConfiguration(options))
            .putAllPathConfigs(mMetaMaster.getPathConfiguration(options))
            .build(), "getConfiguration", "options=%s", responseObserver, options);
  }

  @Override
  public void setPathConfiguration(SetPathConfigurationPRequest request,
      StreamObserver<SetPathConfigurationPResponse> responseObserver) {
    String path = request.getPath();
    String key = request.getKey();
    String value = request.getValue();

    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<SetPathConfigurationPResponse>) () ->
    {
      mMetaMaster.setPathConfiguration(path, PropertyKey.fromString(key), value);
      return SetPathConfigurationPResponse.getDefaultInstance();
    }, "setPathConfiguration", "request=%s", responseObserver, request);
  }

  @Override
  public void removePathConfiguration(RemovePathConfigurationPRequest request,
      StreamObserver<RemovePathConfigurationPResponse> responseObserver) {
    String path = request.getPath();
    List<String> keys = request.getKeysList();

    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<RemovePathConfigurationPResponse>) () -> {
      Set<PropertyKey> keySet = new HashSet<>();
      for (String key : keys) {
        keySet.add(PropertyKey.fromString(key));
      }
      mMetaMaster.removePathConfiguration(path, keySet);
      return RemovePathConfigurationPResponse.getDefaultInstance();
    }, "removePathConfiguration", "request=%s", responseObserver, request);
  }
}
