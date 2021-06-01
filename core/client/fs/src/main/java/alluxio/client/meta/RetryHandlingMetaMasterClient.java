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

package alluxio.client.meta;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.BackupStatusPRequest;
import alluxio.grpc.CheckpointPOptions;
import alluxio.grpc.GetConfigReportPOptions;
import alluxio.grpc.GetMasterInfoPOptions;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterInfoField;
import alluxio.grpc.MetaMasterClientServiceGrpc;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;
import alluxio.wire.BackupStatus;
import alluxio.wire.ConfigCheckReport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the meta master.
 */
@ThreadSafe
public class RetryHandlingMetaMasterClient extends AbstractMasterClient
    implements MetaMasterClient {
  private static final Logger RPC_LOG = LoggerFactory.getLogger(MetaMasterClient.class);
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
  public BackupStatus backup(BackupPRequest backupRequest) throws IOException {
    return retryRPC(() -> BackupStatus.fromProto(mClient.backup(backupRequest)),
        RPC_LOG, "Backup", "backupRequest=%s", backupRequest);
  }

  @Override
  public BackupStatus getBackupStatus(UUID backupId) throws IOException {
    return retryRPC(() -> BackupStatus.fromProto(mClient.getBackupStatus(
        BackupStatusPRequest.newBuilder().setBackupId(backupId.toString()).build())),
        RPC_LOG, "GetBackupStatus", "backupId=%s", backupId);
  }

  @Override
  public ConfigCheckReport getConfigReport() throws IOException {
    return retryRPC(() -> ConfigCheckReport.fromProto(
        mClient.getConfigReport(GetConfigReportPOptions.getDefaultInstance()).getReport()),
        RPC_LOG, "GetConfigReport", "");
  }

  @Override
  public MasterInfo getMasterInfo(final Set<MasterInfoField> fields)
      throws IOException {
    return retryRPC(() -> mClient
        .getMasterInfo(GetMasterInfoPOptions.newBuilder().addAllFilter(fields).build())
        .getMasterInfo(), RPC_LOG, "GetMasterInfo", "fields=%s", fields);
  }

  @Override
  public String checkpoint() throws IOException {
    return retryRPC(() -> mClient
        .checkpoint(CheckpointPOptions.newBuilder().build()).getMasterHostname(),
        RPC_LOG, "Checkpoint", "");
  }
}
