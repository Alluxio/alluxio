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

package alluxio.proxy;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.grpc.BuildVersion;
import alluxio.grpc.MetaMasterProxyServiceGrpc;
import alluxio.grpc.ProxyHeartbeatPOptions;
import alluxio.grpc.ProxyHeartbeatPRequest;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;
import alluxio.wire.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the meta master.
 */
@ThreadSafe
public class RetryHandlingMetaMasterProxyClient extends AbstractMasterClient {
  private static final Logger RPC_LOG =
      LoggerFactory.getLogger(RetryHandlingMetaMasterProxyClient.class);
  private MetaMasterProxyServiceGrpc.MetaMasterProxyServiceBlockingStub mClient = null;
  private final Address mProxyAddress;
  private final long mStartTimeMs;

  /**
   * Creates a new meta master client.
   *
   * @param proxyAddress address of the proxy
   * @param conf master client configuration
   * @param startTimeMs start timestamp
   */
  public RetryHandlingMetaMasterProxyClient(
      Address proxyAddress, MasterClientContext conf, long startTimeMs) {
    super(conf);
    mProxyAddress = proxyAddress;
    mStartTimeMs = startTimeMs;
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.META_MASTER_PROXY_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.META_MASTER_PROXY_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.META_MASTER_PROXY_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = MetaMasterProxyServiceGrpc.newBlockingStub(mChannel);
  }

  /**
   * Sends a heartbeat to the primary master.
   */
  public void proxyHeartbeat() throws IOException {
    BuildVersion version = BuildVersion.newBuilder().setVersion(RuntimeConstants.VERSION)
        .setRevision(RuntimeConstants.REVISION_SHORT).build();
    ProxyHeartbeatPOptions options = ProxyHeartbeatPOptions.newBuilder()
        .setProxyAddress(mProxyAddress.toProto())
        .setStartTime(mStartTimeMs)
        .setVersion(version).build();
    retryRPC(() -> mClient.withDeadlineAfter(
        mContext.getClusterConf().getMs(
            PropertyKey.USER_RPC_RETRY_MAX_DURATION), TimeUnit.MILLISECONDS)
        .proxyHeartbeat(ProxyHeartbeatPRequest.newBuilder().setOptions(options).build()),
        RPC_LOG, "ProxyHeartbeat", "options=%s", options);
  }
}

