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
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.BackupStatusPRequest;
import alluxio.grpc.CheckpointPOptions;
import alluxio.grpc.ClientMetrics;
import alluxio.grpc.GetConfigReportPOptions;
import alluxio.grpc.GetMasterInfoPOptions;
import alluxio.grpc.GetMetricsPOptions;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterInfoField;
import alluxio.grpc.MetaMasterClientServiceGrpc;
import alluxio.grpc.MetricValue;
import alluxio.grpc.MetricsHeartbeatPOptions;
import alluxio.grpc.MetricsHeartbeatPRequest;
import alluxio.grpc.MetricsMasterClientServiceGrpc;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;
import alluxio.wire.BackupStatus;
import alluxio.wire.ConfigCheckReport;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the meta master.
 */
@ThreadSafe
public class RetryHandlingMetricsMasterClient extends AbstractMasterClient
    implements MetricsMasterClient {
  private MetricsMasterClientServiceGrpc.MetricsMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new meta master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingMetricsMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.META_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.META_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.META_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = MetricsMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public void clearMetrics(ClearMetricsPRequest clearMetricsRequest) throws IOException {
    return retryRPC(() -> BackupStatus.fromProto(mClient.clearMetrics(clearMetricsRequest)));
  }

  /**
   * The method the worker should periodically execute to heartbeat back to the master.
   *
   * @param metrics a list of client metrics
   */
  public void heartbeat(final List<ClientMetrics> metrics) throws IOException {
    connect();
    try {
      MetricsHeartbeatPRequest.Builder request = MetricsHeartbeatPRequest.newBuilder();
      request.setOptions(MetricsHeartbeatPOptions.newBuilder()
          .addAllClientMetrics(metrics).build());
      mClient.metricsHeartbeat(request.build());
    } catch (io.grpc.StatusRuntimeException e) {
      disconnect();
      throw new UnavailableException(e);
    }
  }
}
