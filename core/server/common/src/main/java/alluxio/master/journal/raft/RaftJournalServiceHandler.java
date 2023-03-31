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
import alluxio.grpc.LatestSnapshotInfoPRequest;
import alluxio.grpc.RaftJournalServiceGrpc;
import alluxio.grpc.SnapshotData;
import alluxio.grpc.SnapshotMetadata;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.TarUtils;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * RPC handler for raft journal service.
 */
public class RaftJournalServiceHandler extends RaftJournalServiceGrpc.RaftJournalServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(RaftJournalServiceHandler.class);
  private final int mSnapshotReplicationChunkSize = (int) Configuration.getBytes(
      PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLICATION_CHUNK_SIZE);
  private final int mSnapshotCompressionLevel =
      Configuration.getInt(PropertyKey.MASTER_METASTORE_ROCKS_CHECKPOINT_COMPRESSION_LEVEL);

  private final StateMachineStorage mStateMachineStorage;
  private volatile long mLastSnapshotUploadDurationMs = -1;
  private volatile long mLastSnapshotUploadSize = -1;
  private volatile long mLastSnapshotUploadDiskSize = -1;

  /**
   * @param storage the storage that the state machine uses for its snapshots
   */
  public RaftJournalServiceHandler(StateMachineStorage storage) {
    mStateMachineStorage = storage;

    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_UPLOAD_DURATION_MS.getName(),
        () -> mLastSnapshotUploadDurationMs);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_UPLOAD_SIZE.getName(),
        () -> mLastSnapshotUploadSize);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_UPLOAD_DISK_SIZE.getName(),
        () -> mLastSnapshotUploadDiskSize);
  }

  @Override
  public void requestLatestSnapshotInfo(LatestSnapshotInfoPRequest request,
                                        StreamObserver<SnapshotMetadata> responseObserver) {
    LOG.info("Received request for latest snapshot info");
    if (Context.current().isCancelled()) {
      responseObserver.onError(
          Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      return;
    }
    SnapshotInfo snapshot = mStateMachineStorage.getLatestSnapshot();
    SnapshotMetadata.Builder metadata = SnapshotMetadata.newBuilder();
    if (snapshot == null) {
      LOG.info("No snapshot to send");
      metadata.setExists(false);
    } else {
      LOG.info("Found snapshot {}", snapshot.getTermIndex());
      metadata.setExists(true)
          .setSnapshotTerm(snapshot.getTerm())
          .setSnapshotIndex(snapshot.getIndex());
    }
    responseObserver.onNext(metadata.build());
    responseObserver.onCompleted();
  }

  @Override
  public void requestLatestSnapshotData(SnapshotMetadata request,
                                     StreamObserver<SnapshotData> responseObserver) {
    TermIndex index = TermIndex.valueOf(request.getSnapshotTerm(), request.getSnapshotIndex());
    LOG.info("Received request for snapshot data {}", index);
    if (Context.current().isCancelled()) {
      responseObserver.onError(
          Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      return;
    }

    String snapshotDirName = SimpleStateMachineStorage
        .getSnapshotFileName(request.getSnapshotTerm(), request.getSnapshotIndex());
    Path snapshotPath = new File(mStateMachineStorage.getSnapshotDir(), snapshotDirName).toPath();
    final long[] totalBytesSent = {0L}; // needs to be effectively final
    long diskSize;
    LOG.info("Begin snapshot upload of {}", index);
    Instant start = Instant.now();
    try (OutputStream snapshotOutStream = new OutputStream() {
      byte[] mBuffer = new byte[mSnapshotReplicationChunkSize];
      int mBufferPosition = 0;

      @Override
      public void write(int b) {
        mBuffer[mBufferPosition++] = (byte) b;
        if (mBufferPosition == mBuffer.length) {
          flushBuffer();
        }
      }

      @Override
      public void close() {
        if (mBufferPosition > 0) {
          flushBuffer();
        }
      }

      private void flushBuffer() {
        // avoids copy
        ByteString bytes = UnsafeByteOperations.unsafeWrap(mBuffer, 0, mBufferPosition);
        mBuffer = new byte[mSnapshotReplicationChunkSize];
        LOG.trace("Sending chunk of size {}: {}", mBufferPosition, bytes.toByteArray());
        responseObserver.onNext(SnapshotData.newBuilder().setChunk(bytes).build());
        totalBytesSent[0] += mBufferPosition;
        mBufferPosition = 0;
      }
    }) {
      diskSize = TarUtils.writeTarGz(snapshotPath, snapshotOutStream, mSnapshotCompressionLevel);
    } catch (Exception e) {
      LOG.warn("Failed to upload snapshot {}", index, e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asRuntimeException());
      return;
    }
    responseObserver.onCompleted();
    // update last duration and duration timer metrics
    mLastSnapshotUploadDurationMs = Duration.between(start, Instant.now()).toMillis();
    MetricsSystem.timer(MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_UPLOAD_TIMER.getName())
        .update(mLastSnapshotUploadDurationMs, TimeUnit.MILLISECONDS);
    LOG.info("Total milliseconds to upload {}: {}", index, mLastSnapshotUploadDurationMs);
    // update uncompressed snapshot size metric
    mLastSnapshotUploadDiskSize = diskSize;
    MetricsSystem.histogram(
            MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_UPLOAD_DISK_HISTOGRAM.getName())
        .update(mLastSnapshotUploadDiskSize);
    LOG.debug("Total snapshot uncompressed bytes for {}: {}", index, mLastSnapshotUploadDiskSize);
    // update compressed snapshot size (aka size sent over the network)
    mLastSnapshotUploadSize = totalBytesSent[0];
    MetricsSystem.histogram(MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_UPLOAD_HISTOGRAM.getName())
        .update(mLastSnapshotUploadSize);
    LOG.info("Total bytes sent for {}: {}", index, mLastSnapshotUploadSize);
    LOG.info("Uploaded snapshot {}", index);
  }
}
