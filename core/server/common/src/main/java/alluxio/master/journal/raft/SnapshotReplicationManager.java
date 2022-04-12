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

import alluxio.ClientContext;
import alluxio.collections.Pair;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.AbortedException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.DownloadSnapshotPRequest;
import alluxio.grpc.DownloadSnapshotPResponse;
import alluxio.grpc.GetSnapshotInfoRequest;
import alluxio.grpc.GetSnapshotInfoResponse;
import alluxio.grpc.GetSnapshotRequest;
import alluxio.grpc.JournalQueryRequest;
import alluxio.grpc.JournalQueryResponse;
import alluxio.grpc.QuorumServerState;
import alluxio.grpc.SnapshotData;
import alluxio.grpc.SnapshotMetadata;
import alluxio.grpc.UploadSnapshotPRequest;
import alluxio.grpc.UploadSnapshotPResponse;
import alluxio.master.MasterClientContext;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.ClientIpAddressInjector;
import alluxio.util.LogUtils;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.MessageLite;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class for managing snapshot replication among masters.
 * It manages two snapshot replication workflows - worker to master and master to worker.
 *
 * 1. Worker to Master
 * When a raft leader needs a snapshot, instead of taking snapshot locally it copies a recent
 * snapshot from one of the followers.
 *
 * Workflow:
 *
 * - Ratis calls leader state machine to take a snapshot
 * - leader gets snapshot metadata from follower
 * - leader pick one of the the follower and send a request for copying the snapshot
 * - follower receives the request and calls the leader raft journal service to upload the snapshot
 * - after the upload completes, leader remembers the temporary snapshot location and index
 * - Ratis calls the leader state machine again to take a snapshot
 * - leader moves the temporary snapshot to the journal snapshot folder and returns snapshot index
 *
 * 2. Master to Worker
 * When a raft follower receives a notification to download a snapshot, it downloads the latest
 * snapshot from the leader.
 *
 * Workflow:
 *
 * - Ratis leader determines one of the follower needs a snapshot because it misses journal entries
 *   from a long time ago
 * - Ratis leader notifies Ratis follower to install a snapshot from leader, the follower calls the
 *   Alluxio state machine to fulfill this request
 * - the follower state machine calls the snapshot manager which calls the raft journal service from
 *   leader to download a snapshot
 * - after the downloads completes, follower moves the file to snapshot directory and gives Ratis
 *   the snapshot index
 */
public class SnapshotReplicationManager {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotReplicationManager.class);

  private final SimpleStateMachineStorage mStorage;
  private final RaftJournalSystem mJournalSystem;
  private volatile SnapshotInfo mDownloadedSnapshot;
  private final PriorityQueue<Pair<SnapshotMetadata, RaftPeerId>> mSnapshotCandidates;

  private enum DownloadState {
    /** No snapshot download is in progress. */
    IDLE,

    /** Snapshot information is requested from available followers. */
    REQUEST_INFO,

    /** The latest snapshot data is requested from one of the followers. */
    REQUEST_DATA,

    /** The latest snapshot is being downloaded from one of the followers. */
    STREAM_DATA,

    /** A snapshot is downloaded and ready for installation. */
    DOWNLOADED,

    /** A snapshot is being installed to the journal storage. */
    INSTALLING,
  }

  private final AtomicReference<DownloadState> mDownloadState =
      new AtomicReference<>(DownloadState.IDLE);

  /**
   * @param journalSystem the raft journal system
   * @param storage the snapshot storage
   */
  public SnapshotReplicationManager(RaftJournalSystem journalSystem,
      SimpleStateMachineStorage storage) {
    mStorage = storage;
    mJournalSystem = journalSystem;
    mSnapshotCandidates = new PriorityQueue<>((pair1, pair2) -> {
      SnapshotMetadata first = pair1.getFirst();
      SnapshotMetadata second = pair2.getFirst();
      // deliberately reversing the compare order to have bigger numbers rise to the top
      // bigger terms and indexes means a more recent snapshot
      if (first.getSnapshotTerm() == second.getSnapshotTerm()) {
        return Long.compare(second.getSnapshotIndex(), first.getSnapshotIndex());
      }
      return Long.compare(second.getSnapshotTerm(), first.getSnapshotTerm());
    });
  }

  /**
   * Downloads and installs a snapshot from the leader.
   *
   * @return a future with the term index of the installed snapshot
   */
  public CompletableFuture<TermIndex> installSnapshotFromLeader() {
    if (mJournalSystem.isLeader()) {
      return RaftJournalUtils.completeExceptionally(
          new IllegalStateException("Abort snapshot installation after becoming a leader"));
    }
    if (!transitionState(DownloadState.IDLE, DownloadState.STREAM_DATA)) {
      return RaftJournalUtils.completeExceptionally(
          new IllegalStateException("State is not IDLE when starting a snapshot installation"));
    }
    try {
      RaftJournalServiceClient client = createJournalServiceClient();
      String address = String.valueOf(client.getAddress());
      SnapshotDownloader<DownloadSnapshotPRequest, DownloadSnapshotPResponse> observer =
          SnapshotDownloader.forFollower(mStorage, address);
      Timer.Context ctx = MetricsSystem
          .timer(MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_DOWNLOAD_TIMER.getName()).time();
      client.downloadSnapshot(observer);
      return observer.getFuture().thenApplyAsync((termIndex) -> {
        ctx.close();
        mDownloadedSnapshot = observer.getSnapshotToInstall();
        transitionState(DownloadState.STREAM_DATA, DownloadState.DOWNLOADED);
        long index = installDownloadedSnapshot();
        if (index == RaftLog.INVALID_LOG_INDEX) {
          throw new CompletionException(new RuntimeException(
              String.format("Failed to install the downloaded snapshot %s", termIndex)));
        }
        if (index != termIndex.getIndex()) {
          throw new CompletionException(new IllegalStateException(
              String.format("Mismatched snapshot installed - downloaded %d, installed %d",
                  termIndex.getIndex(), index)));
        }
        return termIndex;
      }).whenComplete((termIndex, throwable) -> {
        if (throwable != null) {
          LOG.error("Unexpected exception downloading snapshot from leader {}.", address,
              throwable);
          transitionState(DownloadState.STREAM_DATA, DownloadState.IDLE);
        }
        client.close();
      });
    } catch (Exception e) {
      transitionState(DownloadState.STREAM_DATA, DownloadState.IDLE);
      return RaftJournalUtils.completeExceptionally(e);
    }
  }

  /**
   * Sends a snapshot to the leader.
   *
   * @throws IOException if error occurs while initializing the data stream
   */
  public void sendSnapshotToLeader() throws IOException {
    if (mJournalSystem.isLeader()) {
      throw new IllegalStateException("Server is no longer a follower");
    }
    LOG.debug("Checking latest snapshot to send");
    SnapshotInfo snapshot = mStorage.getLatestSnapshot();
    if (snapshot == null) {
      throw new NotFoundException("No snapshot available");
    }

    SnapshotUploader<UploadSnapshotPRequest, UploadSnapshotPResponse> snapshotUploader =
        SnapshotUploader.forFollower(mStorage, snapshot);
    RaftJournalServiceClient client = createJournalServiceClient();
    LOG.info("Sending stream request to {} for snapshot {}", client.getAddress(),
        snapshot.getTermIndex());
    StreamObserver<UploadSnapshotPRequest> requestObserver =
        client.uploadSnapshot(snapshotUploader);
    requestObserver.onNext(UploadSnapshotPRequest.newBuilder()
        .setData(SnapshotData.newBuilder()
            .setSnapshotTerm(snapshot.getTerm())
            .setSnapshotIndex(snapshot.getIndex())
            .setOffset(0))
        .build());
    snapshotUploader.getCompletionFuture().whenComplete((info, t) -> {
      client.close();
    });
  }

  /**
   * Attempts to copy a snapshot from one of the followers.
   *
   * The leader state machine calls this method regularly when it needs a new snapshot.
   * To avoid blocking normal journal operations, This method always returns a value immediately
   * without waiting for download to finish:
   *
   * - If no download is in progress, it schedules a new download asynchronously and returns
   * {@link RaftLog#INVALID_LOG_INDEX}.
   * - If a download is in progress, it returns {@link RaftLog#INVALID_LOG_INDEX} immediately.
   * - If a download is completed, it moves the downloaded file to the snapshot directory and
   * returns the snapshot index.
   *
   * @return the index of the downloaded snapshot, or {@link RaftLog#INVALID_LOG_INDEX}
   * if no snapshot is installed.
   */
  public long maybeCopySnapshotFromFollower() {
    if (mDownloadState.get() == DownloadState.DOWNLOADED) {
      return installDownloadedSnapshot();
    }
    if (mDownloadState.get() == DownloadState.IDLE) {
      CompletableFuture.runAsync(this::requestSnapshotFromFollowers);
    }
    return RaftLog.INVALID_LOG_INDEX;
  }

  /**
   * Receives a snapshot from follower.
   *
   * @param responseStreamObserver the response stream observer
   * @return the request stream observer
   */
  public StreamObserver<UploadSnapshotPRequest> receiveSnapshotFromFollower(
      StreamObserver<UploadSnapshotPResponse> responseStreamObserver) {
    String followerIp = ClientIpAddressInjector.getIpAddress();
    LOG.info("Received upload snapshot request from follower {}", followerIp);
    SnapshotDownloader<UploadSnapshotPResponse, UploadSnapshotPRequest> observer =
        SnapshotDownloader.forLeader(mStorage, responseStreamObserver,
            followerIp);
    if (!transitionState(DownloadState.REQUEST_DATA, DownloadState.STREAM_DATA)) {
      responseStreamObserver.onCompleted();
      return observer;
    }
    observer.getFuture()
        .thenApply(termIndex -> {
          mDownloadedSnapshot = observer.getSnapshotToInstall();
          transitionState(DownloadState.STREAM_DATA, DownloadState.DOWNLOADED);
          return termIndex;
        }).exceptionally(e -> {
          LOG.error("Unexpected exception downloading snapshot from follower {}.", followerIp, e);
          // this allows the leading master to request other followers for their snapshots. It
          // previously collected information about other snapshots in requestInfo(). If no other
          // snapshots are available requestData() will return false and mDownloadState will be IDLE
          transitionState(DownloadState.STREAM_DATA, DownloadState.REQUEST_DATA);
          CompletableFuture.runAsync(this::requestSnapshotFromFollowers);
          return null;
        });
    return observer;
  }

  /**
   * Handles snapshot requests.
   *
   * @param queryRequest the query request
   * @return the response message, or null if the request is not handled
   * @throws IOException if any error occurred while handling the request
   */
  public Message handleRequest(JournalQueryRequest queryRequest) throws IOException {
    if (queryRequest.hasSnapshotInfoRequest()) {
      SnapshotInfo latestSnapshot = mStorage.getLatestSnapshot();
      if (latestSnapshot == null) {
        LOG.debug("No snapshot to send");
        return toMessage(GetSnapshotInfoResponse.getDefaultInstance());
      }
      JournalQueryResponse response = JournalQueryResponse.newBuilder()
          .setSnapshotInfoResponse(GetSnapshotInfoResponse.newBuilder().setLatest(
              toSnapshotMetadata(latestSnapshot.getTermIndex())))
          .build();
      LOG.debug("Sent snapshot info response {}", response);
      return toMessage(response);
    }
    if (queryRequest.hasSnapshotRequest()) {
      LOG.debug("Start sending snapshot to leader");
      sendSnapshotToLeader();
      return Message.EMPTY;
    }
    return null;
  }

  /**
   * Sends a snapshot to a follower.
   *
   * @param responseObserver the response stream observer
   * @return the request stream observer
   */
  public StreamObserver<DownloadSnapshotPRequest> sendSnapshotToFollower(
      StreamObserver<DownloadSnapshotPResponse> responseObserver) {
    SnapshotInfo snapshot = mStorage.getLatestSnapshot();
    LOG.debug("Received snapshot download request from {}", ClientIpAddressInjector.getIpAddress());
    SnapshotUploader<DownloadSnapshotPResponse, DownloadSnapshotPRequest> requestStreamObserver =
        SnapshotUploader.forLeader(mStorage, snapshot, responseObserver);
    if (snapshot == null) {
      responseObserver.onError(Status.NOT_FOUND
          .withDescription("Cannot find a valid snapshot to download.")
          .asException());
      return requestStreamObserver;
    }
    responseObserver.onNext(DownloadSnapshotPResponse.newBuilder()
        .setData(SnapshotData.newBuilder()
            .setSnapshotTerm(snapshot.getTerm())
            .setSnapshotIndex(snapshot.getIndex())
            .setOffset(0))
        .build());
    return requestStreamObserver;
  }

  private static Message toMessage(MessageLite value) {
    return Message.valueOf(
        UnsafeByteOperations.unsafeWrap(value.toByteString().asReadOnlyByteBuffer()));
  }

  private SnapshotMetadata toSnapshotMetadata(TermIndex value) {
    return value == null ? null :
        SnapshotMetadata.newBuilder()
            .setSnapshotTerm(value.getTerm())
            .setSnapshotIndex(value.getIndex())
            .build();
  }

  private boolean transitionState(DownloadState expected, DownloadState update) {
    if (!mDownloadState.compareAndSet(expected, update)) {
      LOG.warn("Failed to transition from {} to {}: current state is {}",
          expected, update, mDownloadState.get());
      return false;
    }
    LOG.debug("Successfully transitioned from {} to {}", expected, update);
    return true;
  }

  /**
   * Installs a downloaded snapshot in the journal snapshot directory.
   *
   * @return the index of the installed snapshot
   */
  private long installDownloadedSnapshot() {
    if (!transitionState(DownloadState.DOWNLOADED, DownloadState.INSTALLING)) {
      return RaftLog.INVALID_LOG_INDEX;
    }
    File tempFile = null;
    try (Timer.Context ctx = MetricsSystem
        .timer(MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_INSTALL_TIMER.getName()).time()) {
      SnapshotInfo snapshot = mDownloadedSnapshot;
      if (snapshot == null) {
        throw new IllegalStateException("Snapshot is not completed");
      }
      FileInfo fileInfo = snapshot.getFiles().get(0);
      tempFile = fileInfo.getPath().toFile();
      if (!tempFile.exists()) {
        throw new FileNotFoundException(String.format("Snapshot file %s is not found", tempFile));
      }
      SnapshotInfo latestSnapshot = mStorage.getLatestSnapshot();
      TermIndex lastInstalled = latestSnapshot == null ? null : latestSnapshot.getTermIndex();
      TermIndex downloaded = snapshot.getTermIndex();
      if (lastInstalled != null && downloaded.compareTo(lastInstalled) < 0) {
        throw new AbortedException(
            String.format("Snapshot to be installed %s is older than current snapshot %s",
                downloaded, lastInstalled));
      }
      final File snapshotFile = mStorage.getSnapshotFile(
          downloaded.getTerm(), downloaded.getIndex());
      LOG.debug("Moving temp snapshot {} to file {}", tempFile, snapshotFile);
      MD5FileUtil.saveMD5File(snapshotFile, fileInfo.getFileDigest());
      if (!tempFile.renameTo(snapshotFile)) {
        throw new IOException(String.format("Failed to rename %s to %s", tempFile, snapshotFile));
      }
      mStorage.loadLatestSnapshot();
      LOG.info("Completed storing snapshot at {} to file {}", downloaded, snapshotFile);
      return downloaded.getIndex();
    } catch (Exception e) {
      LOG.error("Failed to install snapshot", e);
      if (tempFile != null) {
        tempFile.delete();
      }
      return RaftLog.INVALID_LOG_INDEX;
    } finally {
      transitionState(DownloadState.INSTALLING, DownloadState.IDLE);
    }
  }

  /**
   * Finds a follower with latest snapshot and sends a request to download it.
   */
  private void requestSnapshotFromFollowers() {
    if (mDownloadState.get() == DownloadState.IDLE) {
      if (!transitionState(DownloadState.IDLE, DownloadState.REQUEST_INFO)) {
        return;
      }
      // we want fresh info not polluted by older requests. This ensures that requestData() requests
      // from at most # followers before requesting new info. Otherwise, the candidate queue might
      // grow indefinitely.
      mSnapshotCandidates.clear();
      requestInfo();
      transitionState(DownloadState.REQUEST_INFO, DownloadState.REQUEST_DATA);
    }
    if (mDownloadState.get() == DownloadState.REQUEST_DATA) {
      if (!requestData()) {
        transitionState(DownloadState.REQUEST_DATA, DownloadState.IDLE);
      }
    }
  }

  private void requestInfo() {
    Preconditions.checkState(mDownloadState.get() == DownloadState.REQUEST_INFO);
    try {
      SingleFileSnapshotInfo latestSnapshot = mStorage.getLatestSnapshot();
      SnapshotMetadata snapshotMetadata = latestSnapshot == null ? null :
          SnapshotMetadata.newBuilder()
              .setSnapshotTerm(latestSnapshot.getTerm())
              .setSnapshotIndex(latestSnapshot.getIndex())
              .build();
      // build SnapshotInfoRequests
      Map<RaftPeerId, CompletableFuture<RaftClientReply>> jobs = mJournalSystem
          .getQuorumServerInfoList()
          .stream()
          .filter(server -> server.getServerState() == QuorumServerState.AVAILABLE)
          .map(server -> RaftJournalUtils.getPeerId(
              server.getServerAddress().getHost(),
              server.getServerAddress().getRpcPort()))
          .filter(peerId -> !peerId.equals(mJournalSystem.getLocalPeerId()))
          .collect(Collectors.toMap(Function.identity(),
              peerId -> mJournalSystem.sendMessageAsync(peerId, toMessage(JournalQueryRequest
                  .newBuilder()
                  .setSnapshotInfoRequest(GetSnapshotInfoRequest.getDefaultInstance())
                  .build()))));
      // query all secondary masters for information about their latest snapshot
      for (Map.Entry<RaftPeerId, CompletableFuture<RaftClientReply>> job : jobs.entrySet()) {
        RaftPeerId peerId = job.getKey();
        try {
          RaftClientReply reply = job.getValue().get();
          if (reply.getException() != null) {
            throw reply.getException();
          }
          JournalQueryResponse response = JournalQueryResponse.parseFrom(
              reply.getMessage().getContent().asReadOnlyByteBuffer());
          if (!response.hasSnapshotInfoResponse()) {
            throw new IOException("Invalid response for GetSnapshotInfoRequest " + response);
          }
          LOG.debug("Received snapshot info from follower {} - {}", peerId, response);
          SnapshotMetadata latest = response.getSnapshotInfoResponse().getLatest();
          if (snapshotMetadata == null
              || (latest.getSnapshotTerm() >= snapshotMetadata.getSnapshotTerm())
              && latest.getSnapshotIndex() > snapshotMetadata.getSnapshotIndex()) {
            mSnapshotCandidates.add(new Pair<>(latest, peerId));
          }
        } catch (Exception e) {
          LOG.warn("Error while requesting snapshot info from {}: {}", peerId, e.toString());
        }
      }
    } catch (Exception e) {
      LogUtils.warnWithException(LOG, "Failed to request snapshot info from followers", e);
    }
  }

  private boolean requestData() {
    Preconditions.checkState(mDownloadState.get() == DownloadState.REQUEST_DATA);
    // request snapshots from the most recent to least recent
    while (!mSnapshotCandidates.isEmpty()) {
      Pair<SnapshotMetadata, RaftPeerId> candidate = mSnapshotCandidates.poll();
      SnapshotMetadata metadata = candidate.getFirst();
      RaftPeerId peerId = candidate.getSecond();
      LOG.info("Request data from follower {} for snapshot (t: {}, i: {})",
          peerId, metadata.getSnapshotTerm(), metadata.getSnapshotIndex());
      try {
        RaftClientReply reply = mJournalSystem.sendMessageAsync(peerId,
                toMessage(JournalQueryRequest.newBuilder()
                    .setSnapshotRequest(GetSnapshotRequest.getDefaultInstance()).build()))
            .get();
        if (reply.getException() != null) {
          throw reply.getException();
        }
        return true;
      } catch (Exception e) {
        LOG.warn("Failed to request snapshot data from {}: {}", peerId, e);
      }
    }
    // return failure if there are no more candidates to ask snapshot from
    return false;
  }

  @VisibleForTesting
  synchronized RaftJournalServiceClient createJournalServiceClient()
      throws AlluxioStatusException {
    RaftJournalServiceClient client = new RaftJournalServiceClient(MasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    client.connect();
    return client;
  }
}
