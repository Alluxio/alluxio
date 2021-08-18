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

import alluxio.RpcUtils;

import alluxio.grpc.GetQuorumInfoPRequest;
import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.JournalMasterClientServiceGrpc;
import alluxio.grpc.RemoveQuorumServerPRequest;
import alluxio.grpc.RemoveQuorumServerPResponse;
import alluxio.grpc.ResetPrioritiesPRequest;
import alluxio.grpc.ResetPrioritiesPResponse;
import alluxio.grpc.TransferLeadershipPRequest;
import alluxio.grpc.TransferLeadershipPResponse;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a gRPC handler for journal master RPCs invoked by an Alluxio client.
 */
public class JournalMasterClientServiceHandler
    extends JournalMasterClientServiceGrpc.JournalMasterClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(JournalMasterClientServiceHandler.class);

  private JournalMaster mJournalMaster;

  /**
   * Creates gRPC service handler for JobMaster service.
   *
   * @param journalMaster the journal master
   */
  public JournalMasterClientServiceHandler(JournalMaster journalMaster) {
    mJournalMaster = journalMaster;
  }

  @Override
  public void getQuorumInfo(GetQuorumInfoPRequest request,
      StreamObserver<GetQuorumInfoPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> mJournalMaster.getQuorumInfo(), "getQuorumInfo", "request=%s",
        responseObserver, request);
  }

  @Override
  public void removeQuorumServer(RemoveQuorumServerPRequest request,
      StreamObserver<RemoveQuorumServerPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mJournalMaster.removeQuorumServer(request.getServerAddress());
      return RemoveQuorumServerPResponse.getDefaultInstance();
    }, "removeQuorumServer", "request=%s", responseObserver, request);
  }

  @Override
  public void transferLeadership(TransferLeadershipPRequest request,
      StreamObserver<TransferLeadershipPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mJournalMaster.transferLeadership(request.getServerAddress());
      return TransferLeadershipPResponse.getDefaultInstance();
    }, "transferLeadership", "request=%s", responseObserver, request);
  }

  @Override
  public void resetPriorities(ResetPrioritiesPRequest request,
      StreamObserver<ResetPrioritiesPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mJournalMaster.resetPriorities();
      return ResetPrioritiesPResponse.getDefaultInstance();
    }, "resetPriorities", "request=%s", responseObserver, request);
  }
}
