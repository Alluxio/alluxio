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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.DownloadSnapshotPRequest;
import alluxio.grpc.DownloadSnapshotPResponse;
import alluxio.grpc.SnapshotData;
import alluxio.grpc.UploadSnapshotPRequest;
import alluxio.grpc.UploadSnapshotPResponse;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.IOUtils;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

/**
 * A stream observer for uploading a snapshot.
 *
 * @param <S> the message type to send
 * @param <R> the message type to receive
 */
public class SnapshotUploader<S, R>
    implements StreamObserver<R>, ClientResponseObserver<S, R> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotUploader.class);
  private static final int SNAPSHOT_CHUNK_SIZE = (int) ServerConfiguration.getBytes(
      PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLICATION_CHUNK_SIZE);

  private final Function<SnapshotData, S> mDataMessageBuilder;
  private final Function<R, Long> mOffsetGetter;
  private final File mSnapshotFile;
  private final long mLength;
  private final SnapshotInfo mSnapshotInfo;
  private long mOffset = 0;
  private StreamObserver<S> mStream;

  /**
   * Builds a stream for leader to upload a snapshot.
   *
   * @param storage the snapshot storage
   * @param snapshot the snapshot to upload
   * @param stream the download stream
   * @return the upload stream for leader
   */
  public static SnapshotUploader<DownloadSnapshotPResponse, DownloadSnapshotPRequest> forLeader(
      SimpleStateMachineStorage storage, SnapshotInfo snapshot,
      StreamObserver<DownloadSnapshotPResponse> stream) {
    return new SnapshotUploader<>(storage, snapshot, stream,
        data -> DownloadSnapshotPResponse.getDefaultInstance().toBuilder().setData(data).build(),
        DownloadSnapshotPRequest::getOffsetReceived);
  }

  /**
   * Builds a stream for follower to upload a snapshot.
   *
   * @param storage the snapshot storage
   * @param snapshot the snapshot to upload
   * @return the upload stream for follower
   */
  public static SnapshotUploader<UploadSnapshotPRequest, UploadSnapshotPResponse> forFollower(
      SimpleStateMachineStorage storage, SnapshotInfo snapshot) {
    return new SnapshotUploader<>(storage, snapshot, null,
        data -> UploadSnapshotPRequest.getDefaultInstance().toBuilder().setData(data).build(),
        UploadSnapshotPResponse::getOffsetReceived);
  }

  private SnapshotUploader(SimpleStateMachineStorage storage, SnapshotInfo snapshot,
      StreamObserver<S> stream,
      Function<SnapshotData, S> buildFunc, Function<R, Long> offsetGetter) {
    mSnapshotInfo = snapshot;
    mDataMessageBuilder = buildFunc;
    mOffsetGetter = offsetGetter;
    mSnapshotFile = storage.getSnapshotFile(snapshot.getTerm(), snapshot.getIndex());
    mLength = mSnapshotFile.length();
    mStream = stream;
  }

  @Override
  public void onNext(R value) {
    try {
      onNextInternal(value);
    } catch (Exception e) {
      LOG.error("Error occurred while sending snapshot", e);
      mStream.onError(e);
    }
  }

  private void onNextInternal(R value) throws IOException {
    LOG.debug("Received request {}", value);
    if (mStream == null) {
      throw new IllegalStateException("No request stream assigned");
    }
    if (!mSnapshotFile.exists()) {
      throw new FileNotFoundException(
          String.format("Snapshot file %s does not exist", mSnapshotFile.getPath()));
    }
    long offsetReceived = mOffsetGetter.apply(value);
    // TODO(feng): implement better flow control
    if (mOffset != offsetReceived) {
      throw new InvalidArgumentException(
          String.format("Received mismatched offset: %d. Expect %d", offsetReceived, mOffset));
    }
    LOG.debug("Streaming data at {}", mOffset);
    try (InputStream is = new FileInputStream(mSnapshotFile)) {
      is.skip(mOffset);
      boolean eof = false;
      int chunkSize = SNAPSHOT_CHUNK_SIZE;
      long available = mLength - mOffset;
      if (available <= SNAPSHOT_CHUNK_SIZE) {
        eof = true;
        chunkSize = (int) available;
      }
      byte[] buffer = new byte[chunkSize];
      IOUtils.readFully(is, buffer);
      LOG.debug("Read {} bytes from file {}", chunkSize, mSnapshotFile);
      mStream.onNext(mDataMessageBuilder.apply(SnapshotData.newBuilder()
          .setOffset(mOffset)
          .setEof(eof)
          .setChunk(UnsafeByteOperations.unsafeWrap(buffer))
          .setSnapshotTerm(mSnapshotInfo.getTerm())
          .setSnapshotIndex(mSnapshotInfo.getIndex())
          .build()));
      mOffset += chunkSize;
      LOG.debug("Uploaded total {} bytes of file {}", mOffset, mSnapshotFile);
    }
  }

  @Override
  public void onError(Throwable t) {
    LOG.error("Error sending snapshot {} at {}", mSnapshotFile, mOffset, t);
  }

  @Override
  public void onCompleted() {
    LOG.debug("Received onComplete");
    mStream.onCompleted();
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<S> requestStream) {
    mStream = requestStream;
  }
}
