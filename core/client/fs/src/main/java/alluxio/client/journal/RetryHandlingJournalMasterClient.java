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

package alluxio.client.journal;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GetQuorumInfoPRequest;
import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.JournalMasterClientServiceGrpc;
import alluxio.grpc.NetAddress;
import alluxio.grpc.RemoveQuorumServerPRequest;
import alluxio.grpc.ResetPrioritiesPRequest;
import alluxio.grpc.ServiceType;
import alluxio.grpc.TransferLeadershipPRequest;
import alluxio.master.MasterClientContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for the gRPC client to interact with the journal master, used by alluxio clients.
 */
public class RetryHandlingJournalMasterClient extends AbstractMasterClient
    implements JournalMasterClient {
  private static final Logger RPC_LOG = LoggerFactory.getLogger(JournalMasterClient.class);
  private JournalMasterClientServiceGrpc.JournalMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new journal master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingJournalMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.JOURNAL_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.JOURNAL_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.JOURNAL_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = JournalMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public GetQuorumInfoPResponse getQuorumInfo() throws AlluxioStatusException {
    return retryRPC(() -> mClient.getQuorumInfo(GetQuorumInfoPRequest.getDefaultInstance()),
        RPC_LOG, "GetQuorumInfo",  "");
  }

  @Override
  public void removeQuorumServer(NetAddress serverAddress) throws AlluxioStatusException {
    retryRPC(() -> mClient.removeQuorumServer(
        RemoveQuorumServerPRequest.newBuilder().setServerAddress(serverAddress).build()),
        RPC_LOG, "RemoveQuorumServer",  "serverAddress=%s", serverAddress);
  }

  @Override
  public void transferLeadership(NetAddress newLeaderNetAddress) throws AlluxioStatusException {
    retryRPC(() -> mClient.transferLeadership(
        TransferLeadershipPRequest.newBuilder().setServerAddress(newLeaderNetAddress).build()),
        RPC_LOG, "TransferLeadership", "serverAddress=%s", newLeaderNetAddress);
  }

  @Override
  public void resetPriorities() throws AlluxioStatusException {
    retryRPC(() -> mClient.resetPriorities(ResetPrioritiesPRequest.getDefaultInstance()),
            RPC_LOG, "ResetPriorities", "");
  }
}
