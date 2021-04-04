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
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ClearMetricsPRequest;
import alluxio.grpc.ClearMetricsPResponse;
import alluxio.grpc.ClearMetricsResponse;
import alluxio.grpc.ClientMetrics;
import alluxio.grpc.GetMetricsPOptions;
import alluxio.grpc.GetMetricsPResponse;
import alluxio.grpc.MetricValue;
import alluxio.grpc.MetricsHeartbeatPOptions;
import alluxio.grpc.MetricsHeartbeatPRequest;
import alluxio.grpc.MetricsHeartbeatPResponse;
import alluxio.grpc.MetricsMasterClientServiceGrpc;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the metrics master.
 */
@ThreadSafe
public class RetryHandlingMetricsMasterClient extends AbstractMasterClient
    implements MetricsMasterClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(RetryHandlingMetricsMasterClient.class);
  private MetricsMasterClientServiceGrpc.MetricsMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new metrics master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingMetricsMasterClient(MasterClientContext conf) {
    super(conf);
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

  @Override
  public void clearMetrics() throws IOException {
    retryRPC(() -> {
      ClearMetricsPResponse response = mClient.clearMetrics(ClearMetricsPRequest.newBuilder().build());
      if (LOG.isDebugEnabled()) {
        LOG.debug("clearMetrics response has {} bytes", response.getSerializedSize());
      }
      return null;
      }, LOG, "ClearMetrics", "");
  }

  @Override
  public void heartbeat(final List<ClientMetrics> metrics) throws IOException {
    connect();
    try {
      MetricsHeartbeatPRequest.Builder request = MetricsHeartbeatPRequest.newBuilder();
      request.setOptions(MetricsHeartbeatPOptions.newBuilder()
          .addAllClientMetrics(metrics).build());
      MetricsHeartbeatPResponse response = mClient.metricsHeartbeat(request.build());
      if (LOG.isDebugEnabled()) {
        LOG.debug("metricsHeartbeat response is {} bytes", response.getSerializedSize());
      }
    } catch (io.grpc.StatusRuntimeException e) {
      disconnect();
      throw new UnavailableException(e);
    }
  }

  @Override
  public Map<String, MetricValue> getMetrics() throws AlluxioStatusException {
    return retryRPC(
        () -> {
            GetMetricsPResponse response = mClient.getMetrics(GetMetricsPOptions.getDefaultInstance());
            if (LOG.isDebugEnabled()) {
              LOG.debug("getMetrics response has {} bytes, {} metrics", response.getSerializedSize(),
                      response.getMetricsCount());
            }
            return response.getMetricsMap();
            }, LOG, "GetMetrics", "");
  }
}
