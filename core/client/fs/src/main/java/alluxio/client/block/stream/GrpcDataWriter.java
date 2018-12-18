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
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CanceledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Chunk;
import alluxio.grpc.RequestType;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteRequestCommand;
import alluxio.grpc.WriteResponse;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.LockResource;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A gRPC data writer that streams a full block or a UFS file to a gRPC data server.
 *
 * Protocol:
 * 1. The client streams data chunks (start from pos 0) to the server. The client pauses if the
 *    client buffer is full, resumes if the buffer is not full.
 * 2. The server reads chunks from the stream and writes them to the block worker. See the server
 *    side implementation for details.
 * 3. The client can either complete or cancel the stream to end the write request. The
 *    client has to wait for the response from the data server for the EOF or CANCEL packet to make
 *    sure that the server has cleaned its states.
 * 4. To make it simple to handle errors, the stream is closed if any error occurs.
 *
 * NOTE: this class is NOT threadsafe. Do not call cancel/close while some other threads are
 * writing.
 */
@NotThreadSafe
public final class GrpcDataWriter implements PacketWriter {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataWriter.class);

  private static final long WRITE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
  private static final long CLOSE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_WRITER_CLOSE_TIMEOUT_MS);
  /** Uses a long flush timeout since flush in S3 streaming upload may take a long time. */
  private static final long FLUSH_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_WRITER_FLUSH_TIMEOUT);

  private final FileSystemContext mContext;
  private final BlockWorkerClient mClient;
  private final WorkerNetAddress mAddress;
  private final long mLength;
  private final WriteRequestCommand mPartialRequest;
  private final ClientCallStreamObserver<WriteRequest> mRequestStream;
  private final long mPacketSize;

  private boolean mClosed;

  /**
   * Uses to gurantee the operation ordering.
   *
   * NOTE: {@link Channel#writeAndFlush(Object)} is async.
   * Netty I/O thread executes the {@link ChannelFutureListener#operationComplete(Future)}
   * before writing any new message to the wire, which may introduce another layer of ordering.
   */
  private final ReentrantLock mLock = new ReentrantLock();

  @GuardedBy("mLock")
  private long mPosWritten;

  /**
   * The next pos to queue to the buffer.
   */
  @GuardedBy("mLock")
  private long mPosToQueue;
  @GuardedBy("mLock")
  private Throwable mError;
  @GuardedBy("mLock")
  private boolean mDone;
  @GuardedBy("mLock")
  private boolean mEOFSent;
  @GuardedBy("mLock")
  private boolean mCancelSent;
  /** This condition is met if mError != null or mDone = true. */
  private final Condition mDoneOrFailed = mLock.newCondition();
  /** This condition is met if mError != null or flush is completed. */
  private final Condition mFlushedOrFailed = mLock.newCondition();
  /** This condition is met if mError != null or client is ready to send data. */
  private final Condition mReadyOrFailed = mLock.newCondition();

  /**
   * @param context the file system context
   * @param address the data server address
   * @param id the block or UFS ID
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param type type of the write request
   * @param options the options of the output stream
   * @return an instance of {@link GrpcDataWriter}
   */
  public static GrpcDataWriter create(FileSystemContext context, WorkerNetAddress address,
      long id, long length, RequestType type, OutStreamOptions options)
      throws IOException {
    long packetSize = Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
    BlockWorkerClient grpcClient = context.acquireBlockWorkerClient(address);
    return new GrpcDataWriter(context, address, id, length, packetSize, type, options,
        grpcClient);
  }

  /**
   * Creates an instance of {@link GrpcDataWriter}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param id the block or UFS file Id
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param packetSize the packet size
   * @param type type of the write request
   * @param options details of the write request which are constant for all requests
   * @param client the block worker client
   */
  private GrpcDataWriter(FileSystemContext context, final WorkerNetAddress address, long id,
      long length, long packetSize, RequestType type, OutStreamOptions options,
      BlockWorkerClient client) {
    mContext = context;
    mAddress = address;
    mLength = length;
    WriteRequestCommand.Builder builder =
        WriteRequestCommand.newBuilder().setId(id).setTier(options.getWriteTier()).setType(type);
    if (type == RequestType.UFS_FILE) {
      Protocol.CreateUfsFileOptions ufsFileOptions =
          Protocol.CreateUfsFileOptions.newBuilder().setUfsPath(options.getUfsPath())
              .setOwner(options.getOwner()).setGroup(options.getGroup())
              .setMode(options.getMode().toShort()).setMountId(options.getMountId())
              .setAcl(ProtoUtils.toProto(options.getAcl()))
              .build();
      builder.setCreateUfsFileOptions(ufsFileOptions);
    }
    // two cases to use UFS_FALLBACK_BLOCK endpoint:
    // (1) this writer is created by the fallback of a short-circuit writer, or
    boolean alreadyFallback = type == RequestType.UFS_FALLBACK_BLOCK;
    // (2) the write type is async when UFS tier is enabled.
    boolean possibleToFallback = type == RequestType.ALLUXIO_BLOCK
        && options.getWriteType() == alluxio.client.WriteType.ASYNC_THROUGH
        && Configuration.getBoolean(PropertyKey.USER_FILE_UFS_TIER_ENABLED);
    if (alreadyFallback || possibleToFallback) {
      // Overwrite to use the fallback-enabled endpoint in case (2)
      builder.setType(RequestType.UFS_FALLBACK_BLOCK);
      Protocol.CreateUfsBlockOptions ufsBlockOptions = Protocol.CreateUfsBlockOptions.newBuilder()
          .setMountId(options.getMountId())
          .setFallback(alreadyFallback).build();
      builder.setCreateUfsBlockOptions(ufsBlockOptions);
    }
    mPartialRequest = builder.buildPartial();
    mPacketSize = packetSize;
    mClient = client;
    StreamObserver<WriteResponse> responseObserver = new WriteStreamObserver();
    mRequestStream = (ClientCallStreamObserver<WriteRequest>) mClient.writeBlock(responseObserver);
    mRequestStream.onNext(WriteRequest.newBuilder().setCommand(
        mPartialRequest.toBuilder()).build());
  }

  @Override
  public long pos() {
    try (LockResource lr = new LockResource(mLock)) {
      return mPosToQueue;
    }
  }

  @Override
  public void writePacket(final ByteBuf buf) throws IOException {
    try (LockResource lr = new LockResource(mLock)) {
      while (true) {
        if (mError != null) {
          Throwables.propagateIfPossible(mError, IOException.class);
          throw AlluxioStatusException.fromCheckedException(mError);
        }
        if (mRequestStream.isReady()) {
          mPosToQueue += buf.readableBytes();
          break;
        }
        try {
          if (!mReadyOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            throw new DeadlineExceededException(
                String.format("Timeout writing to %s for request %s after %dms.",
                    mAddress, mPartialRequest, WRITE_TIMEOUT_MS));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CanceledException(e);
        }
      }
    }
    mRequestStream.onNext(WriteRequest.newBuilder().setCommand(mPartialRequest).setChunk(
        Chunk.newBuilder().setData(ByteString.copyFrom(buf.nioBuffer())).build()).build());
  }

  /**
   * Notifies the server UFS fallback endpoint to start writing a new block by resuming the given
   * number of bytes from block store.
   *
   * @param pos number of bytes already written to block store
   */
  public void writeFallbackInitPacket(long pos) {
    Preconditions.checkState(mPartialRequest.getType() == RequestType.UFS_FALLBACK_BLOCK);
    Protocol.CreateUfsBlockOptions ufsBlockOptions = mPartialRequest.getCreateUfsBlockOptions()
        .toBuilder().setBytesInBlockStore(pos).build();
    WriteRequest writeRequest = WriteRequest.newBuilder().setCommand(
        mPartialRequest.toBuilder().setOffset(0).setCreateUfsBlockOptions(ufsBlockOptions))
        .build();
    try (LockResource lr = new LockResource(mLock)) {
      mPosToQueue = pos;
    }
    mRequestStream.onNext(writeRequest);
  }

  @Override
  public void cancel() {
    if (mClosed) {
      return;
    }
    sendCancel();
  }

  @Override
  public void flush() throws IOException {
    try (LockResource lr = new LockResource(mLock)) {
      if (mEOFSent || mCancelSent || mPosToQueue == 0) {
        return;
      }
      WriteRequest writeRequest = WriteRequest.newBuilder()
          .setCommand(mPartialRequest.toBuilder().setOffset(mPosToQueue).setFlush(true))
          .build();
      mRequestStream.onNext(writeRequest);
      if (mPosToQueue != mPosWritten &&
          !mFlushedOrFailed.await(FLUSH_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new DeadlineExceededException(
            String.format("Timeout flushing to %s for request %s after %dms.",
                mAddress, mPartialRequest, FLUSH_TIMEOUT_MS));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CanceledException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    sendEof();
    mLock.lock();
    try {
      while (true) {
        if (mDone) {
          return;
        }
        try {
          if (mError != null) {
            throw new UnavailableException(
                "Failed to write data packet due to " + mError.getMessage(),
                mError);
          }
          if (!mDoneOrFailed.await(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            mRequestStream.onCompleted();
            throw new DeadlineExceededException(String.format(
                "Timeout closing PacketWriter to %s for request %s after %dms.",
                mAddress, mPartialRequest, CLOSE_TIMEOUT_MS));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CanceledException(e);
        }
      }
    } finally {
      mLock.unlock();
      mContext.releaseBlockWorkerClient(mAddress, mClient);
      mClosed = true;
    }
  }

  /**
   * Sends an EOF message to end the write request of the stream.
   */
  private void sendEof() {
    try (LockResource lr = new LockResource(mLock)) {
      if (mEOFSent || mCancelSent) {
        return;
      }
      mEOFSent = true;
    }
    mRequestStream.onCompleted();
  }

  /**
   * Sends a CANCEL packet to end the write request of the stream.
   */
  private void sendCancel() {
    try (LockResource lr = new LockResource(mLock)) {
      if (mEOFSent || mCancelSent) {
        return;
      }
      mCancelSent = true;
    }
    mRequestStream.cancel("Request is cancelled by user.", null);
  }

  @Override
  public int packetSize() {
    return (int) mPacketSize;
  }

  /**
   * Updates the channel exception to be the given exception e, or adds e to suppressed exceptions.
   *
   * @param e Exception received
   */
  @GuardedBy("mLock")
  private void updateException(Throwable e) {
    if (mError == null || mError == e) {
      mError = e;
    } else {
      mError.addSuppressed(e);
    }
  }

  // An observer for write response stream that handles async events.
  private final class WriteStreamObserver implements ClientResponseObserver<WriteRequest, WriteResponse> {

    @Override
    public void onNext(WriteResponse response) {
      // currently only flush expects a response
      Preconditions.checkState(response.hasOffset(), "missing offset in flush response");
      try (LockResource lr = new LockResource(mLock)) {
        mPosWritten = response.getOffset();
        Preconditions.checkState(mPosToQueue == mPosWritten,
            "No data should be sent before flush is acked.");
        Preconditions.checkState(mPosToQueue <= mLength);
        mFlushedOrFailed.signal();
      }
    }

    @Override
    public void onError(Throwable t) {
      try (LockResource lr = new LockResource(mLock)) {
        updateException(t);
        mDoneOrFailed.signal();
        mFlushedOrFailed.signal();
        mReadyOrFailed.signal();
      }
    }

    @Override
    public void onCompleted() {
      try (LockResource lr = new LockResource(mLock)) {
        mDone = true;
        mDoneOrFailed.signal();
        mFlushedOrFailed.signal();
      }
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<WriteRequest> requestStream) {
      requestStream.setOnReadyHandler(() -> {
        try (LockResource lr = new LockResource(mLock)) {
          mReadyOrFailed.signal();
        }
      });
    }
  }
}

