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

package alluxio.client.block.stream;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local packet writer that simply writes packets to a local file.
 */
@NotThreadSafe
public final class LocalFilePacketWriter implements PacketWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFilePacketWriter.class);
  private static final long FILE_BUFFER_BYTES =
      Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
  private static final long READ_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
  private final BlockWorkerClient mBlockWorker;
  private final LocalFileBlockWriter mWriter;
  private final long mPacketSize;
  private final CreateLocalBlockRequest mCreateRequest;
  private final OutStreamOptions mOptions;
  private final Closer mCloser;
  private final ResponseObserver mResponseObserver;
  private final ClientCallStreamObserver<CreateLocalBlockRequest> mRequestObserver;

  /** The position to write the next byte at. */
  private long mPos;
  /** The number of bytes reserved on the block worker to hold the block. */
  private long mPosReserved;

  private boolean mClosed = false;

  /**
   * Creates an instance of {@link LocalFilePacketWriter}. This requires the block to be locked
   * beforehand.
   *
   * @param context the file system context
   * @param address the worker network address
   * @param blockId the block ID
   * @param options the output stream options
   * @return the {@link LocalFilePacketWriter} created
   */
  public static LocalFilePacketWriter create(final FileSystemContext context,
      final WorkerNetAddress address,
      long blockId, OutStreamOptions options) throws IOException {
    long packetSize = Configuration.getBytes(PropertyKey.USER_LOCAL_WRITER_PACKET_SIZE_BYTES);

    Closer closer = Closer.create();
    try {
      final BlockWorkerClient blockWorker = context.acquireBlockWorkerClient(address);
      closer.register(new Closeable() {
        @Override
        public void close() throws IOException {
          blockWorker.close();
        }
      });
      CreateLocalBlockRequest.Builder builder =
          CreateLocalBlockRequest.newBuilder().setBlockId(blockId)
              .setTier(options.getWriteTier()).setSpaceToReserve(FILE_BUFFER_BYTES);
      if (options.getWriteType() == WriteType.ASYNC_THROUGH
          && Configuration.getBoolean(PropertyKey.USER_FILE_UFS_TIER_ENABLED)) {
        builder.setCleanupOnFailure(false);
      }
      CreateLocalBlockRequest createRequest = builder.build();
      ResponseObserver responseObserver = new ResponseObserver();
      StreamObserver<CreateLocalBlockRequest> request =
          blockWorker.createLocalBlock(responseObserver);
      request.onNext(createRequest);
      try (LockResource lr = new LockResource(responseObserver.getLock())) {
        if (responseObserver.getResponse() == null
            && !responseObserver.getCreatedOrFailed()
            .await(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new DeadlineExceededException(
              String.format("Timeout waiting for create request to complete %s",
                  createRequest.toString()));
        }
      }
      LocalFileBlockWriter writer =
          closer.register(new LocalFileBlockWriter(responseObserver.getResponse().getPath()));
      return new LocalFilePacketWriter(packetSize, options, blockWorker,
          writer, createRequest, (ClientCallStreamObserver<CreateLocalBlockRequest>) request,
          responseObserver, closer);
    } catch (Exception e) {
      throw CommonUtils.closeAndRethrow(closer, e);
    }
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public int packetSize() {
    return (int) mPacketSize;
  }

  @Override
  public void writePacket(final ByteBuf buf) throws IOException {
    try {
      Preconditions.checkState(!mClosed, "PacketWriter is closed while writing packets.");
      int sz = buf.readableBytes();
      ensureReserved(mPos + sz);
      mPos += sz;
      Preconditions.checkState(mWriter.append(buf) == sz);
    } finally {
      buf.release();
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;

    try {
      mRequestObserver.cancel("Operation canceled by client", null);
    } catch (Exception e) {
      throw mCloser.rethrow(e);
    } finally {
      mCloser.close();
    }
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;

    mCloser.register(new Closeable() {
      @Override
      public void close() throws IOException {
        mRequestObserver.onCompleted();
        try (LockResource lr = new LockResource(mResponseObserver.getLock())) {
          if (!mResponseObserver.isCompleted()
              && !mResponseObserver.getCompletedOrFailed()
              .await(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            throw new DeadlineExceededException(String.format(
                "Timeout closing local file for request %s.", mCreateRequest.toString()));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(String.format(
              "Interrupted while closing local file for request %s.", mCreateRequest.toString()),
              e);
        }
      }
    });
    mCloser.close();
  }

  /**
   * Creates an instance of {@link LocalFilePacketWriter}.
   * @param packetSize the packet size
   * @param options the output stream options
   * @param request
   * @param responseObserver the response observer
   */
  private LocalFilePacketWriter(long packetSize, OutStreamOptions options,
      BlockWorkerClient blockWorker, LocalFileBlockWriter writer,
      CreateLocalBlockRequest createRequest,
      ClientCallStreamObserver<CreateLocalBlockRequest> request, ResponseObserver responseObserver,
      Closer closer) {
    mBlockWorker = blockWorker;
    mCloser = closer;
    mOptions = options;
    mWriter = writer;
    mCreateRequest = createRequest;
    mRequestObserver = request;
    mResponseObserver = responseObserver;
    mPosReserved += FILE_BUFFER_BYTES;
    mPacketSize = packetSize;
  }

  /**
   * Reserves enough space in the block worker.
   *
   * @param pos the pos of the file/block to reserve to
   */
  private void ensureReserved(long pos) throws IOException {
    if (pos <= mPosReserved) {
      return;
    }
    long toReserve = Math.max(pos - mPosReserved, FILE_BUFFER_BYTES);
    mRequestObserver.onNext(mCreateRequest.toBuilder().setSpaceToReserve(toReserve)
          .setOnlyReserveSpace(true).build());
    try (LockResource lr = new LockResource(mResponseObserver.getLock())) {
      if (!mResponseObserver.getReservedOrFailed()
          .await(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new DeadlineExceededException(String.format(
            "Timeout reserving space for request %s.", mCreateRequest.toString()));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(String.format(
          "Interrupted reserving space for request %s.", mCreateRequest.toString()), e);
    }
    mPosReserved += toReserve;
  }

  private static class ResponseObserver implements StreamObserver<CreateLocalBlockResponse> {
    private final ReentrantLock mLock = new ReentrantLock();
    /** This condition is met if mError != null or create response is returned. */
    private final Condition mCreatedOrFailed = mLock.newCondition();
    /** This condition is met if mError != null or reserveSpace response is returned. */
    private final Condition mReservedOrFailed = mLock.newCondition();
    /** This condition is met if mError != null or complete response is returned. */
    private final Condition mCompletedOrFailed = mLock.newCondition();
    private CreateLocalBlockResponse mResponse = null;
    private boolean mCompleted = false;
    private Throwable mError = null;

    @Override
    public void onNext(CreateLocalBlockResponse createLocalBlockResponse) {
      try (LockResource lr = new LockResource(mLock)) {
        if (createLocalBlockResponse.hasPath()) {
          mResponse = createLocalBlockResponse;
          mCreatedOrFailed.signal();
        } else {
          mReservedOrFailed.signal();
        }
      }
    }

    @Override
    public void onError(Throwable throwable) {
      try (LockResource lr = new LockResource(mLock)) {
        mError = throwable;
        mCreatedOrFailed.signal();
        mReservedOrFailed.signal();
        mCompletedOrFailed.signal();
      }
    }

    @Override
    public void onCompleted() {
      try (LockResource lr = new LockResource(mLock)) {
        mCompleted = true;
        mCompletedOrFailed.signal();
      }
    }

    public CreateLocalBlockResponse getResponse() {
      return mResponse;
    }

    public ReentrantLock getLock() {
      return mLock;
    }

    public boolean isCompleted() {
      return mCompleted;
    }

    public Throwable getError() {
      return mError;
    }

    public Condition getCompletedOrFailed() {
      return mCompletedOrFailed;
    }

    public Condition getCreatedOrFailed() {
      return mCreatedOrFailed;
    }

    public Condition getReservedOrFailed() {
      return mReservedOrFailed;
    }
  }
}

