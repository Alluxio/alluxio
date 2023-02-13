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

package alluxio.master.journal.raft;

import alluxio.grpc.DownloadSnapshotPRequest;
import alluxio.grpc.DownloadSnapshotPResponse;
import alluxio.grpc.RaftJournalServiceGrpc;
import alluxio.grpc.SnapshotData;
import alluxio.grpc.SnapshotMetadata;
import alluxio.grpc.UploadSnapshotPRequest;
import alluxio.grpc.UploadSnapshotPResponse;

import com.google.protobuf.Empty;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC handler for raft journal service.
 */
public class RaftJournalServiceHandler extends RaftJournalServiceGrpc.RaftJournalServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(RaftJournalServiceHandler.class);
  private final SnapshotReplicationManager mManager;
  private final RaftJournalSystem mRaftJournalSystem;

  /**
   * @param manager the snapshot replication manager
   * @param raftJournalSystem the raft journal system
   */
  public RaftJournalServiceHandler(
      SnapshotReplicationManager manager,
      RaftJournalSystem raftJournalSystem) {
    mManager = manager;
    mRaftJournalSystem = raftJournalSystem;
    LOG.debug("RaftJournalServiceHandler initialized, journal system {}",
        mRaftJournalSystem);
  }

  @Override
  public void requestLatestSnapshotInfo(Empty request,
      StreamObserver<SnapshotMetadata> responseObserver) {
    // https://grpc.io/blog/deadlines/
    if (Context.current().isCancelled()) {
      responseObserver.onError(
          Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      return;
    }
    SnapshotMetadata.Builder builder =
        SnapshotMetadata.newBuilder().setSnapshotTerm(1).setSnapshotIndex(1);
    mRaftJournalSystem.getStateMachine().ifPresent(statemachine -> {
      SnapshotInfo snapshot = statemachine.getLatestSnapshot();
      builder.setSnapshotTerm(snapshot.getTerm());
      builder.setSnapshotIndex(snapshot.getIndex());
    });
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void downloadLatestSnapshot(Empty request, StreamObserver<SnapshotData> responseObserver) {
    responseObserver.onError(new NotImplementedException("not implemented"));
  }

  @Override
  public StreamObserver<UploadSnapshotPRequest> uploadSnapshot(
      StreamObserver<UploadSnapshotPResponse> responseObserver) {
    return mManager.receiveSnapshotFromFollower(responseObserver);
  }

  @Override
  public StreamObserver<DownloadSnapshotPRequest> downloadSnapshot(
      StreamObserver<DownloadSnapshotPResponse> responseObserver) {
    return mManager.sendSnapshotToFollower(responseObserver);
  }
}
