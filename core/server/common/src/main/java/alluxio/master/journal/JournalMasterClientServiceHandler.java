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
import alluxio.grpc.CrashRatisServerPRequest;
import alluxio.grpc.CrashRatisServerPResponse;
import alluxio.grpc.GetQuorumInfoPRequest;
import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.GetTransferLeaderMessagePRequest;
import alluxio.grpc.GetTransferLeaderMessagePResponse;
import alluxio.grpc.JournalMasterClientServiceGrpc;
import alluxio.grpc.RemoveQuorumServerPRequest;
import alluxio.grpc.RemoveQuorumServerPResponse;
import alluxio.grpc.ResetPrioritiesPRequest;
import alluxio.grpc.ResetPrioritiesPResponse;
import alluxio.grpc.TransferLeadershipPRequest;
import alluxio.grpc.TransferLeadershipPResponse;

import alluxio.master.MasterProcess;
import alluxio.master.journal.raft.RaftJournalSystem;
import io.grpc.stub.StreamObserver;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

/**
 * This class is a gRPC handler for journal master RPCs invoked by an Alluxio client.
 */
public class JournalMasterClientServiceHandler
    extends JournalMasterClientServiceGrpc.JournalMasterClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(JournalMasterClientServiceHandler.class);

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
    RpcUtils.call(LOG, () -> {
      String transferId = mJournalMaster.transferLeadership(request.getServerAddress());
      return TransferLeadershipPResponse.newBuilder().setTransferId(transferId).build();
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

  @Override
  public void getTransferLeaderMessage(GetTransferLeaderMessagePRequest request,
      StreamObserver<GetTransferLeaderMessagePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> mJournalMaster.getTransferLeaderMessage(request.getTransferId()),
            "GetTransferLeaderMessage", "request=%s", responseObserver, request);
  }

  @Override
  public void crashRatisServer(CrashRatisServerPRequest request, StreamObserver<CrashRatisServerPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      try {
        corruptRatisServerInternally();
        return CrashRatisServerPResponse.getDefaultInstance();
      } catch (Exception e) {
        throw new IOException(e);
      }
      }, "CrashRatisServerMessage", "request=%s", responseObserver, request);
  }

  private void corruptRatisServerInternally() throws Exception {
    RaftServer s = getRaftServer();
    System.out.println("Corrupting the Ratis server internal state");
    Class c = s.getClass();
    Method[] allMethods = c.getDeclaredMethods();
    Method target = null;
    for (Method mm : allMethods) {
      if (mm.getName().contains("getImpls")) {
        System.out.println("Method is " + mm);
        target = mm;
      }
    }
    target.setAccessible(true);
    Object implObj = target.invoke(s);
    List<RaftServer.Division> impls = (List<RaftServer.Division>) implObj;
    RaftServer.Division d = impls.get(0);
    SegmentedRaftLog log = (SegmentedRaftLog) d.getRaftLog();
    Field f = log.getClass().getDeclaredField("cache");
    f.setAccessible(true);
    SegmentedRaftLogCache cache = (SegmentedRaftLogCache) f.get(log);
    Field f2 = cache.getClass().getDeclaredField("openSegment");
    f2.setAccessible(true);
    f2.set(cache, null);
    System.out.println("Server is corrupted internally");
    // This should throw an NPE
//    cache.getTotalCacheSize();
  }

  private RaftServer getRaftServer() throws Exception {
    DefaultJournalMaster jm = (DefaultJournalMaster) mJournalMaster;
    Field privateField = DefaultJournalMaster.class.getDeclaredField("mJournalSystem");
    privateField.setAccessible(true);
    RaftJournalSystem journalSystem = (RaftJournalSystem) privateField.get(jm);
    Field pf2 = RaftJournalSystem.class.getDeclaredField("mServer");
    pf2.setAccessible(true);
    RaftServer server = (RaftServer) pf2.get(journalSystem);

    return server;
  }
}
