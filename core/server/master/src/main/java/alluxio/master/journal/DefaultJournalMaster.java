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

package alluxio.master.journal;

import alluxio.Constants;
import alluxio.clock.SystemClock;
import alluxio.grpc.GetNodeStatePResponse;
import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.GetTransferLeaderMessagePResponse;
import alluxio.grpc.GrpcService;
import alluxio.grpc.JournalDomain;
import alluxio.grpc.NetAddress;
import alluxio.grpc.ServiceType;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.master.PrimarySelector;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.util.executor.ExecutorServiceFactories;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation for {@link JournalMaster}. This is used by both master and job_master
 * processes.
 */
public class DefaultJournalMaster extends AbstractMaster implements JournalMaster, NoopJournaled {
  private final JournalDomain mJournalDomain;
  private final JournalSystem mJournalSystem;
  private final PrimarySelector mPrimarySelector;

  /**
   * @param journalDomain domain for the journal
   * @param masterContext the context for Alluxio master
   */
  public DefaultJournalMaster(JournalDomain journalDomain, MasterContext masterContext) {
    super(masterContext, new SystemClock(),
        ExecutorServiceFactories.cachedThreadPool(Constants.JOURNAL_MASTER_NAME));
    mJournalDomain = journalDomain;
    mJournalSystem = masterContext.getJournalSystem();
    mPrimarySelector = masterContext.getPrimarySelector();
  }

  private void checkQuorumOpSupported() {
    if (!(mJournalSystem instanceof RaftJournalSystem)) {
      throw new UnsupportedOperationException(
          "Quorum operations are supported for journal type: EMBEDDED");
    }
    if (!((RaftJournalSystem) (mJournalSystem)).isLeader()) {
      throw new UnsupportedOperationException(
          "Quorum operations are only supported on Raft leader");
    }
  }

  @Override
  public GetQuorumInfoPResponse getQuorumInfo() throws IOException {
    checkQuorumOpSupported();
    return GetQuorumInfoPResponse.newBuilder().setDomain(mJournalDomain)
        .addAllServerInfo(((RaftJournalSystem) mJournalSystem).getQuorumServerInfoList()).build();
  }

  @Override
  public void removeQuorumServer(NetAddress serverAddress) throws IOException {
    checkQuorumOpSupported();
    ((RaftJournalSystem) mJournalSystem).removeQuorumServer(serverAddress);
  }

  @Override
  public String transferLeadership(NetAddress newLeaderAddress) {
    checkQuorumOpSupported();
    return ((RaftJournalSystem) mJournalSystem).transferLeadership(newLeaderAddress);
  }

  @Override
  public void resetPriorities() throws IOException {
    checkQuorumOpSupported();
    ((RaftJournalSystem) mJournalSystem).resetPriorities();
  }

  @Override
  public GetTransferLeaderMessagePResponse getTransferLeaderMessage(String transferId) {
    checkQuorumOpSupported();
    return GetTransferLeaderMessagePResponse.newBuilder()
           .setTransMsg(((RaftJournalSystem) mJournalSystem).getTransferLeaderMessage(transferId))
           .build();
  }

  @Override
  public GetNodeStatePResponse getNodeState() {
    return GetNodeStatePResponse.newBuilder()
        .setNodeState(mPrimarySelector.getState())
        .build();
  }

  @Override
  public String getName() {
    return Constants.JOURNAL_MASTER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(alluxio.grpc.ServiceType.JOURNAL_MASTER_CLIENT_SERVICE,
        new GrpcService(new JournalMasterClientServiceHandler(this)));
    return services;
  }
}
