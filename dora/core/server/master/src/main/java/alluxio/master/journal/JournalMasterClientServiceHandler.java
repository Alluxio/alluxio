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
import alluxio.grpc.GetNodeStatePRequest;
import alluxio.grpc.GetNodeStatePResponse;
import alluxio.grpc.GetQuorumInfoPRequest;
import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.GetTransferLeaderMessagePRequest;
import alluxio.grpc.GetTransferLeaderMessagePResponse;
import alluxio.grpc.JournalMasterClientServiceGrpc;
import alluxio.grpc.RemoveQuorumServerPRequest;
import alluxio.grpc.RemoveQuorumServerPResponse;
import alluxio.grpc.ResetPrioritiesPRequest;
import alluxio.grpc.ResetPrioritiesPResponse;
import alluxio.grpc.TransferLeaderMessage;
import alluxio.grpc.TransferLeadershipPRequest;
import alluxio.grpc.TransferLeadershipPResponse;

import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is a gRPC handler for journal master RPCs invoked by an Alluxio client.
 */
public class JournalMasterClientServiceHandler
    extends JournalMasterClientServiceGrpc.JournalMasterClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(JournalMasterClientServiceHandler.class);

  private final Map<String, String> mTransferLeaderMessages = new ConcurrentHashMap<>();

  private final JournalMaster mJournalMaster;

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
    RpcUtils.call(LOG, mJournalMaster::getQuorumInfo, "getQuorumInfo", "request=%s",
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
    try {
      // using RpcUtils wrapper for metrics tracking
      RpcUtils.callAndReturn(LOG, () -> {
        String transferId = UUID.randomUUID().toString();
        // atomically reserve UUID in map with empty message: if not in use (which is good), it
        // will return null
        while (mTransferLeaderMessages.putIfAbsent(transferId, "") != null) {
          transferId = UUID.randomUUID().toString();
        }
        String message;
        try {
          // return transfer id to caller before initiating transfer of leadership. this is because
          // the leader will close its gRPC server when being demoted
          responseObserver.onNext(
              TransferLeadershipPResponse.newBuilder().setTransferId(transferId).build());
          responseObserver.onCompleted();
          // initiate transfer after replying with transfer ID
          message = mJournalMaster.transferLeadership(request.getServerAddress());
        } catch (Throwable t) {
          message = t.getMessage();
        }
        mTransferLeaderMessages.put(transferId, message);
        return null;
      }, "transferLeadership", false, "request=%s", request);
    } catch (StatusException e) {
      // throws only if above callable throws, which it does not
      LOG.warn("error thrown in transferLeadership rpc, should not be possible", e);
    }
  }

  @Override
  public void resetPriorities(ResetPrioritiesPRequest request,
      StreamObserver<ResetPrioritiesPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mJournalMaster.resetPriorities();
      return ResetPrioritiesPResponse.getDefaultInstance();
    }, "resetPriorities", "request=%s", responseObserver, request);
  }

  @Override
  public void getTransferLeaderMessage(GetTransferLeaderMessagePRequest request,
      StreamObserver<GetTransferLeaderMessagePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetTransferLeaderMessagePResponse.newBuilder()
        .setTransMsg(TransferLeaderMessage.newBuilder()
            .setMsg(mTransferLeaderMessages.getOrDefault(request.getTransferId(), "")))
        .build(),
        "GetTransferLeaderMessage", "request=%s", responseObserver, request);
  }

  @Override
  public void getNodeState(GetNodeStatePRequest request,
       StreamObserver<GetNodeStatePResponse> responseObserver) {
    RpcUtils.call(LOG, mJournalMaster::getNodeState,
        "GetNodeState", "request=%s", responseObserver, request);
  }
}
