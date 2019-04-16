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

package alluxio.client;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.GetConfigReportPOptions;
import alluxio.grpc.GetMasterInfoPOptions;
import alluxio.grpc.GetMetricsPOptions;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterInfoField;
import alluxio.grpc.MetaMasterClientServiceGrpc;
import alluxio.grpc.MetricValue;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;
import alluxio.wire.BackupResponse;
import alluxio.wire.ConfigCheckReport;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the meta master.
 */
@ThreadSafe
public class RetryHandlingMetaMasterClient extends AbstractMasterClient
    implements MetaMasterClient {
  private MetaMasterClientServiceGrpc.MetaMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new meta master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingMetaMasterClient(MasterClientContext conf) {
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
    mClient = MetaMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public BackupResponse backup(BackupPOptions options) throws IOException {
    return retryRPC(() -> BackupResponse.fromProto(mClient.backup(options)));
  }

  @Override
  public ConfigCheckReport getConfigReport() throws IOException {
    return retryRPC(() -> ConfigCheckReport.fromProto(
        mClient.getConfigReport(GetConfigReportPOptions.getDefaultInstance()).getReport()));
  }

  @Override
  public MasterInfo getMasterInfo(final Set<MasterInfoField> fields)
      throws IOException {
    return retryRPC(() -> mClient
        .getMasterInfo(GetMasterInfoPOptions.newBuilder().addAllFilter(fields).build())
        .getMasterInfo());
  }

  @Override
  public Map<String, MetricValue> getMetrics() throws AlluxioStatusException {
    return retryRPC(
        () -> mClient.getMetrics(GetMetricsPOptions.getDefaultInstance()).getMetricsMap());
  }
}
