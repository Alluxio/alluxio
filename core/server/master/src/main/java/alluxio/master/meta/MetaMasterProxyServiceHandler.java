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
import alluxio.grpc.MetaMasterProxyServiceGrpc;
import alluxio.grpc.ProxyHeartbeatPRequest;
import alluxio.grpc.ProxyHeartbeatPResponse;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a gRPC handler for meta master RPCs invoked by an Alluxio standby master.
 */
@NotThreadSafe
public final class MetaMasterProxyServiceHandler
        extends MetaMasterProxyServiceGrpc.MetaMasterProxyServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterProxyServiceHandler.class);

  private final MetaMaster mMetaMaster;

  /**
   * Creates a new instance of {@link MetaMasterProxyServiceHandler}.
   *
   * @param metaMaster the Alluxio meta master
   */
  public MetaMasterProxyServiceHandler(MetaMaster metaMaster) {
    mMetaMaster = metaMaster;
  }

  @Override
  public void proxyHeartbeat(ProxyHeartbeatPRequest request,
                             StreamObserver<ProxyHeartbeatPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mMetaMaster.proxyHeartbeat(request);
      return ProxyHeartbeatPResponse.newBuilder().build();
    }, "proxyHeartbeat", "request=%s", responseObserver, request);
  }
}
