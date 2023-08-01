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
import alluxio.grpc.GetConfigHashPOptions;
import alluxio.grpc.GetConfigHashPResponse;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.grpc.MetaMasterConfigurationServiceGrpc;
import alluxio.wire.ConfigHash;

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
    RpcUtils.call(LOG, () -> {
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
    RpcUtils.call(LOG, () ->
        mMetaMaster.getConfigHash().toProto(), "getConfigHash", "request=%s", responseObserver,
        request);
  }
}
