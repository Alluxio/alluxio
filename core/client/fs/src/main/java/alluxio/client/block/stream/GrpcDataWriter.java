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
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
 *    client has to wait for the complete or cancel response from the data server to make
 *    sure that the server has cleaned its states.
 * 4. To make it simple to handle errors, the stream is closed if any error occurs.
 *
 * NOTE: this class is NOT threadsafe. Do not call cancel/close while some other threads are
 * writing.
 */
@NotThreadSafe
public final class GrpcDataWriter implements DataWriter {
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
  private final long mChunkSize;

  /**
   * Uses to guarantee the operation ordering.
   *
   * NOTE: {@link StreamObserver} events are async.
   * gRPC worker threads executes the response {@link StreamObserver} events.
   */
  private final ReentrantLock mLock = new ReentrantLock();
  private final GrpcBlockingStream<WriteRequest, WriteResponse> mStream;

  /**
   * The next pos to queue to the buffer.
   */
  @GuardedBy("mLock")
  private long mPosToQueue;
  @GuardedBy("mLock")
  private boolean mEOFSent;
  @GuardedBy("mLock")
  private boolean mCancelSent;

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
    long chunkSize = Configuration.getBytes(PropertyKey.USER_NETWORK_WRITER_CHUNK_SIZE_BYTES);
    BlockWorkerClient grpcClient = context.acquireBlockWorkerClient(address);
    return new GrpcDataWriter(context, address, id, length, chunkSize, type, options,
        grpcClient);
  }

  /**
   * Creates an instance of {@link GrpcDataWriter}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param id the block or UFS file Id
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param chunkSize the chunk size
   * @param type type of the write request
   * @param options details of the write request which are constant for all requests
   * @param client the block worker client
   */
  private GrpcDataWriter(FileSystemContext context, final WorkerNetAddress address, long id,
      long length, long chunkSize, RequestType type, OutStreamOptions options,
      BlockWorkerClient client) throws IOException {
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
    mChunkSize = chunkSize;
    mClient = client;
    mStream = new GrpcBlockingStream<>(mClient::writeBlock);
    mStream.send(WriteRequest.newBuilder().setCommand(mPartialRequest.toBuilder()).build(),
        WRITE_TIMEOUT_MS);
  }

  @Override
  public long pos() {
    try (LockResource lr = new LockResource(mLock)) {
      return mPosToQueue;
    }
  }

  @Override
  public void writeChunk(final ByteBuf buf) throws IOException {
    try (LockResource lr = new LockResource(mLock)) {
      mPosToQueue += buf.readableBytes();
    }
    mStream.send(WriteRequest.newBuilder().setCommand(mPartialRequest).setChunk(
        Chunk.newBuilder().setData(ByteString.copyFrom(buf.nioBuffer())).build()).build(),
        WRITE_TIMEOUT_MS);
  }

  /**
   * Notifies the server UFS fallback endpoint to start writing a new block by resuming the given
   * number of bytes from block store.
   *
   * @param pos number of bytes already written to block store
   */
  public void writeFallbackInitRequest(long pos) throws IOException {
    Preconditions.checkState(mPartialRequest.getType() == RequestType.UFS_FALLBACK_BLOCK);
    Protocol.CreateUfsBlockOptions ufsBlockOptions = mPartialRequest.getCreateUfsBlockOptions()
        .toBuilder().setBytesInBlockStore(pos).build();
    WriteRequest writeRequest = WriteRequest.newBuilder().setCommand(
        mPartialRequest.toBuilder().setOffset(0).setCreateUfsBlockOptions(ufsBlockOptions))
        .build();
    try (LockResource lr = new LockResource(mLock)) {
      mPosToQueue = pos;
    }
    mStream.send(writeRequest, WRITE_TIMEOUT_MS);
  }

  @Override
  public void cancel() {
    if (mClient.isShutdown()) {
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
      mStream.send(writeRequest, WRITE_TIMEOUT_MS);
      long posWritten;
      do {
        WriteResponse response = mStream.receive(FLUSH_TIMEOUT_MS);
        if (response == null) {
          throw new UnavailableException(String.format(
              "Flush request %s is not acked before complete.", writeRequest));
        }
        posWritten = response.getOffset();
      } while (mPosToQueue != posWritten);
    }
  }

  @Override
  public void close() throws IOException {
    if (mClient.isShutdown()) {
      return;
    }
    sendEof();
    mLock.lock();
    try {
      while (mStream.receive(CLOSE_TIMEOUT_MS) != null) {
        // wait until receives empty(response stream closed)
      }
    } finally {
      mLock.unlock();
      mContext.releaseBlockWorkerClient(mAddress, mClient);
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
    mStream.close();
  }

  /**
   * Sends a CANCEL message to end the write request of the stream.
   */
  private void sendCancel() {
    try (LockResource lr = new LockResource(mLock)) {
      if (mEOFSent || mCancelSent) {
        return;
      }
      mCancelSent = true;
    }
    mStream.cancel();
  }

  @Override
  public int chunkSize() {
    return (int) mChunkSize;
  }
}

