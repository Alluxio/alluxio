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

package alluxio.client.metrics;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Metric;
import alluxio.grpc.MetricsHeartbeatPOptions;
import alluxio.grpc.MetricsHeartbeatPRequest;
import alluxio.grpc.MetricsMasterClientServiceGrpc;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientConfig;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.RetryUtils;
import alluxio.util.network.NetworkAddressUtils;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A client to use for interacting with a metrics master.
 */
@ThreadSafe
public class MetricsMasterClient extends AbstractMasterClient {
  private MetricsMasterClientServiceGrpc.MetricsMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new metrics master client.
   *
   * @param conf master client configuration
   */
  public MetricsMasterClient(MasterClientConfig conf) {
    super(conf, null, RetryUtils::defaultMetricsClientRetry);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.METRICS_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.METRICS_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.METRICS_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = MetricsMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  /**
   * The method the worker should periodically execute to heartbeat back to the master.
   *
   * @param metrics a list of client metrics
   */
  public void heartbeat(final List<Metric> metrics) throws IOException {
    connect();
    try {
      MetricsHeartbeatPRequest.Builder request = MetricsHeartbeatPRequest.newBuilder();
      request.setClientId(MetricsSystem.getAppId());
      request.setHostname(NetworkAddressUtils.getClientHostName());
      request.setOptions(MetricsHeartbeatPOptions.newBuilder().addAllMetrics(metrics).build());
      mClient.metricsHeartbeat(request.build());
    } catch (io.grpc.StatusRuntimeException e) {
      throw new UnavailableException(e);
    }
  }
}
