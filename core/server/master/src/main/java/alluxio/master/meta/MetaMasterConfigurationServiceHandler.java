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
import alluxio.grpc.GetConfigHashPOptions;
import alluxio.grpc.GetConfigHashPResponse;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.grpc.MetaMasterConfigurationServiceGrpc;
import alluxio.grpc.RemovePathConfigurationPRequest;
import alluxio.grpc.RemovePathConfigurationPResponse;
import alluxio.grpc.SetPathConfigurationPRequest;
import alluxio.grpc.SetPathConfigurationPResponse;
import alluxio.grpc.UpdateConfigurationPRequest;
import alluxio.grpc.UpdateConfigurationPResponse;
import alluxio.wire.ConfigHash;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * This class is a gRPC handler for meta master RPCs.
 */
public final class MetaMasterConfigurationServiceHandler
    extends MetaMasterConfigurationServiceGrpc.MetaMasterConfigurationServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(MetaMasterConfigurationServiceHandler.class);

  private final MetaMaster mMetaMaster;
  /**
   * The cached response of GetConfiguration to save serialization cost when configurations are
   * not updated.
   */
  private volatile GetConfigurationPResponse mClusterConf;
  private volatile GetConfigurationPResponse mPathConf;

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
      GetConfigurationPResponse clusterConf = mClusterConf;
      GetConfigurationPResponse pathConf = mPathConf;
      GetConfigurationPResponse.Builder builder = GetConfigurationPResponse.newBuilder();
      ConfigHash hash = mMetaMaster.getConfigHash();
      if (!options.getIgnoreClusterConf()) {
        if (clusterConf == null
            || !clusterConf.getClusterConfigHash().equals(hash.getClusterConfigHash())) {
          clusterConf = mMetaMaster.getConfiguration(options.toBuilder()
              .setIgnorePathConf(true).build()).toProto();
          mClusterConf = clusterConf;
        }
        builder.addAllClusterConfigs(clusterConf.getClusterConfigsList());
        builder.setClusterConfigHash(clusterConf.getClusterConfigHash());
      }
      if (!options.getIgnorePathConf()) {
        if (pathConf == null
            || !pathConf.getPathConfigHash().equals(hash.getPathConfigHash())) {
          pathConf = mMetaMaster.getConfiguration(options.toBuilder()
              .setIgnoreClusterConf(true).build()).toProto();
          mPathConf = pathConf;
        }
        builder.putAllPathConfigs(pathConf.getPathConfigsMap());
        builder.setPathConfigHash(pathConf.getPathConfigHash());
      }
      return builder.build();
    }, "getConfiguration", "request=%s", responseObserver, options);
  }

  @Override
  public void getConfigHash(GetConfigHashPOptions request,
      StreamObserver<GetConfigHashPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetConfigHashPResponse>) () ->
        mMetaMaster.getConfigHash().toProto(), "getConfigHash", "request=%s", responseObserver,
        request);
  }

  @Override
  public void setPathConfiguration(SetPathConfigurationPRequest request,
      StreamObserver<SetPathConfigurationPResponse> responseObserver) {
    String path = request.getPath();
    Map<String, String> properties = request.getPropertiesMap();

    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<SetPathConfigurationPResponse>) () ->
    {
      Map<PropertyKey, String> props = new HashMap<>();
      properties.forEach((key, value) -> props.put(PropertyKey.fromString(key), value));
      mMetaMaster.setPathConfiguration(path, props);
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
        if (keys.isEmpty()) {
          mMetaMaster.removePathConfiguration(path);
        } else {
          mMetaMaster.removePathConfiguration(path, new HashSet<>(keys));
        }
        return RemovePathConfigurationPResponse.getDefaultInstance();
      }, "removePathConfiguration", "request=%s", responseObserver, request);
  }

  @Override
  public void updateConfiguration(
      UpdateConfigurationPRequest request,
      StreamObserver<UpdateConfigurationPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Map<String, Boolean> result =
          mMetaMaster.updateConfiguration(request.getPropertiesMap());
      return UpdateConfigurationPResponse.newBuilder()
          .putAllStatus(result)
          .build();
    }, "updateConfiguration", "request=%s", responseObserver, request);
  }
}
