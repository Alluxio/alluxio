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
import alluxio.grpc.SnapshotData;
import alluxio.grpc.UploadSnapshotPRequest;
import alluxio.grpc.UploadSnapshotPResponse;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A stream observer for downloading a snapshot.
 *
 * @param <S> type of the message to send
 * @param <R> type of the message to receive
 */
public class SnapshotDownloader<S, R> implements ClientResponseObserver<S, R> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDownloader.class);

  private final SimpleStateMachineStorage mStorage;
  private final CompletableFuture<TermIndex> mFuture = new CompletableFuture<>();
  private final Function<Long, S> mMessageBuilder;
  private final Function<R, SnapshotData> mDataGetter;
  private final String mSource;

  /** The term and index for the latest journal entry included in the snapshot. */
  private TermIndex mTermIndex;
  private File mTempFile;
  private FileOutputStream mOutputStream;
  private long mBytesWritten = 0;
  private StreamObserver<S> mStream;
  private SnapshotInfo mSnapshotToInstall;

  /**
   * Builds a stream for leader to download a snapshot.
   *
   * @param storage the snapshot storage
   * @param stream the response stream
   * @param source the source of the snapshot
   * @return the download stream for leader
   */
  public static SnapshotDownloader<UploadSnapshotPResponse, UploadSnapshotPRequest> forLeader(
      SimpleStateMachineStorage storage, StreamObserver<UploadSnapshotPResponse> stream,
      String source) {
    return new SnapshotDownloader<>(storage,
        offset -> UploadSnapshotPResponse.newBuilder().setOffsetReceived(offset).build(),
        UploadSnapshotPRequest::getData, stream, source);
  }

  /**
   * Builds a stream for follower to download a snapshot.
   *
   * @param storage the snapshot storage
   * @param source the source of the snapshot
   * @return the download stream for follower
   */
  public static SnapshotDownloader<DownloadSnapshotPRequest, DownloadSnapshotPResponse>
      forFollower(SimpleStateMachineStorage storage, String source) {
    return new SnapshotDownloader<>(storage,
        offset -> DownloadSnapshotPRequest.newBuilder().setOffsetReceived(offset).build(),
        DownloadSnapshotPResponse::getData, null, source);
  }

  private SnapshotDownloader(SimpleStateMachineStorage storage, Function<Long, S> messageBuilder,
      Function<R, SnapshotData> dataGetter, StreamObserver<S> stream, String source) {
    mStorage = storage;
    mMessageBuilder = messageBuilder;
    mDataGetter = dataGetter;
    mStream = stream;
    mSource = source;
  }

  @Override
  public void onNext(R response) {
    try {
      onNextInternal(response);
    } catch (Exception e) {
      mStream.onError(e);
      mFuture.completeExceptionally(e);
      cleanup();
    }
  }

  private void cleanup() {
    if (mOutputStream != null) {
      try {
        mOutputStream.close();
      } catch (IOException ioException) {
        LOG.error("Error closing snapshot file {}", mTempFile, ioException);
      }
    }
    if (mTempFile != null && !mTempFile.delete()) {
      LOG.error("Error deleting snapshot file {}", mTempFile.getPath());
    }
  }

  private void onNextInternal(R response) throws IOException {
    TermIndex termIndex = TermIndex.valueOf(
        mDataGetter.apply(response).getSnapshotTerm(),
        mDataGetter.apply(response).getSnapshotIndex());
    if (mTermIndex == null) {
      LOG.info("Downloading new snapshot {} from {}", termIndex, mSource);
      mTermIndex = termIndex;
      // start a new file
      mTempFile = RaftJournalUtils.createTempSnapshotFile(mStorage);

      mTempFile.deleteOnExit();
      mStream.onNext(mMessageBuilder.apply(0L));
    } else {
      if (!termIndex.equals(mTermIndex)) {
        throw new IOException(String.format(
            "Mismatched term index when downloading the snapshot. expected: %s actual: %s",
            mTermIndex, termIndex));
      }
      if (!mDataGetter.apply(response).hasChunk()) {
        throw new IOException(String.format(
            "A chunk for file %s is missing from the response %s.", mTempFile, response));
      }
      // write the chunk
      if (mOutputStream == null) {
        LOG.info("Start writing to temporary file {}", mTempFile.getPath());
        mOutputStream = new FileOutputStream(mTempFile);
      }
      long position = mOutputStream.getChannel().position();
      if (position != mDataGetter.apply(response).getOffset()) {
        throw new IOException(
            String.format("Mismatched offset in file %d, expect %d, bytes written %d",
                position, mDataGetter.apply(response).getOffset(), mBytesWritten));
      }
      mOutputStream.write(mDataGetter.apply(response).getChunk().toByteArray());
      mBytesWritten += mDataGetter.apply(response).getChunk().size();
      LOG.debug("Written {} bytes to snapshot file {}", mBytesWritten, mTempFile.getPath());
      if (mDataGetter.apply(response).getEof()) {
        LOG.debug("Completed writing to temporary file {} with size {}",
            mTempFile.getPath(), mOutputStream.getChannel().position());
        mOutputStream.close();
        mOutputStream = null;
        final MD5Hash digest = MD5FileUtil.computeMd5ForFile(mTempFile);
        mSnapshotToInstall = new SingleFileSnapshotInfo(
            new FileInfo(mTempFile.toPath(), digest),
            mTermIndex.getTerm(), mTermIndex.getIndex());
        mFuture.complete(mTermIndex);
        LOG.info("Finished copying snapshot to local file {}.", mTempFile);
        mStream.onCompleted();
      } else {
        mStream.onNext(mMessageBuilder.apply(mBytesWritten));
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    mFuture.completeExceptionally(t);
    cleanup();
  }

  @Override
  public void onCompleted() {
    if (mOutputStream != null) {
      mFuture.completeExceptionally(
          new IllegalStateException("Request completed with unfinished upload"));
      cleanup();
    }
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<S> requestStream) {
    mStream = requestStream;
  }

  /**
   * @return a future that tracks when the stream is completed
   */
  public CompletableFuture<TermIndex> getFuture() {
    return mFuture;
  }

  /**
   * @return the snapshot information if it is downloaded completely, or null otherwise
   */
  public SnapshotInfo getSnapshotToInstall() {
    return mSnapshotToInstall;
  }
}
