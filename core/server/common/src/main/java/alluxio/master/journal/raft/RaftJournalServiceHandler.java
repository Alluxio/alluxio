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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.DownloadFilePRequest;
import alluxio.grpc.DownloadSnapshotPRequest;
import alluxio.grpc.DownloadSnapshotPResponse;
import alluxio.grpc.FileMetadata;
import alluxio.grpc.LatestSnapshotInfoPRequest;
import alluxio.grpc.RaftJournalServiceGrpc;
import alluxio.grpc.SnapshotData;
import alluxio.grpc.SnapshotMetadata;
import alluxio.grpc.UploadSnapshotPRequest;
import alluxio.grpc.UploadSnapshotPResponse;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * RPC handler for raft journal service.
 */
public class RaftJournalServiceHandler extends RaftJournalServiceGrpc.RaftJournalServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(RaftJournalServiceHandler.class);
  private final int mSnapshotReplicationChunkSize = (int) Configuration.getBytes(
      PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLICATION_CHUNK_SIZE);
  private final SnapshotReplicationManager mManager;
  private final RaftJournalSystem mRaftJournalSystem;

  /**
   * @param manager           the snapshot replication manager
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
  public void requestLatestSnapshotInfo(LatestSnapshotInfoPRequest request,
                                        StreamObserver<SnapshotMetadata> responseObserver) {
    LOG.debug("Received request for latest snapshot info");
    if (invalidResponseObserver(responseObserver)) {
      return;
    }
    SnapshotInfo snapshot = mRaftJournalSystem.getStateMachine().get().getLatestSnapshot();
    SnapshotMetadata.Builder metadata = SnapshotMetadata.newBuilder();
    if (snapshot == null) {
      LOG.debug("No snapshot to send");
      metadata.setExists(false);
    } else {
      LOG.debug("Found snapshot {}", snapshot.getTermIndex());
      List<FileMetadata> fileMetadata = snapshot.getFiles().stream()
          .map(fileInfo -> FileMetadata.newBuilder()
              .setRelativePath(fileInfo.getPath().toString())
              .build())
          .collect(Collectors.toList());
      metadata.setExists(true)
          .setSnapshotTerm(snapshot.getTerm())
          .setSnapshotIndex(snapshot.getIndex())
          .addAllFileMetadataList(fileMetadata);
    }
    responseObserver.onNext(metadata.build());
    responseObserver.onCompleted();
  }

  @Override
  public void downloadLatestSnapshot(DownloadFilePRequest request,
                                     StreamObserver<SnapshotData> plainResponseObserver) {
    if (invalidResponseObserver(plainResponseObserver)) {
      return;
    }
    ServerCallStreamObserver<SnapshotData> responseObserver =
        (ServerCallStreamObserver<SnapshotData>) plainResponseObserver;
    responseObserver.setCompression("gzip");

    StateMachineStorage storage =
        mRaftJournalSystem.getStateMachine().get().getStateMachineStorage();
    String snapshotFileName = SimpleStateMachineStorage
        .getSnapshotFileName(request.getSnapshotTerm(), request.getSnapshotIndex());
    File file1 = new File(snapshotFileName, request.getFileMetadata().getRelativePath());
    File file = new File(storage.getSnapshotDir(), file1.toString());
    LOG.debug("Uploading file {}", file);
    try (InputStream inputStream = Files.newInputStream(file.toPath())) {
      byte[] buffer = new byte[mSnapshotReplicationChunkSize];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        responseObserver.onNext(SnapshotData.newBuilder()
            .setSnapshotTerm(request.getSnapshotTerm())
            .setSnapshotIndex(request.getSnapshotIndex())
            .setChunk(ByteString.copyFrom(buffer, 0, bytesRead))
            .build());
      }
      responseObserver.onCompleted();
      LOG.debug("Successfully uploaded {}", file);
    } catch (Exception e) {
      LOG.debug("Failed to upload file {}", request.getFileMetadata().getRelativePath(), e);
      responseObserver.onError(e);
      responseObserver.onCompleted();
    }
  }

  private <T> boolean invalidResponseObserver(StreamObserver<T> observer) {
    if (Context.current().isCancelled()) {
      observer.onError(
          Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      return true;
    }
    Optional<JournalStateMachine> stateMachine = mRaftJournalSystem.getStateMachine();
    if (!stateMachine.isPresent()) {
      observer.onError(
          new IllegalStateException("Raft journal system does not have a state machine"));
      observer.onCompleted();
      return true;
    }
    return false;
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
