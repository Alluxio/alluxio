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
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local data writer that simply writes packets to a local file.
 */
@NotThreadSafe
public final class LocalFileDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileDataWriter.class);
  private static final long FILE_BUFFER_BYTES =
      Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
  private static final long WRITE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
  private final BlockWorkerClient mBlockWorker;
  private final LocalFileBlockWriter mWriter;
  private final long mChunkSize;
  private final CreateLocalBlockRequest mCreateRequest;
  private final Closer mCloser;
  private final GrpcBlockingStream<CreateLocalBlockRequest, CreateLocalBlockResponse> mStream;

  /** The position to write the next byte at. */
  private long mPos;
  /** The number of bytes reserved on the block worker to hold the block. */
  private long mPosReserved;

  private boolean mClosed = false;

  /**
   * Creates an instance of {@link LocalFileDataWriter}. This requires the block to be locked
   * beforehand.
   *
   * @param context the file system context
   * @param address the worker network address
   * @param blockId the block ID
   * @param options the output stream options
   * @return the {@link LocalFileDataWriter} created
   */
  public static LocalFileDataWriter create(final FileSystemContext context,
      final WorkerNetAddress address,
      long blockId, OutStreamOptions options) throws IOException {
    long chunkSize = Configuration.getBytes(PropertyKey.USER_LOCAL_WRITER_CHUNK_SIZE_BYTES);

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
      GrpcBlockingStream<CreateLocalBlockRequest, CreateLocalBlockResponse> stream =
          new GrpcBlockingStream<>(blockWorker::createLocalBlock);
      stream.send(createRequest, WRITE_TIMEOUT_MS);
      CreateLocalBlockResponse response = stream.receive(WRITE_TIMEOUT_MS);
      Preconditions.checkState(response != null && response.hasPath());
      LocalFileBlockWriter writer =
          closer.register(new LocalFileBlockWriter(response.getPath()));
      return new LocalFileDataWriter(chunkSize, blockWorker,
          writer, createRequest, stream, closer);
    } catch (Exception e) {
      throw CommonUtils.closeAndRethrow(closer, e);
    }
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public int chunkSize() {
    return (int) mChunkSize;
  }

  @Override
  public void writeChunk(final ByteBuf buf) throws IOException {
    try {
      Preconditions.checkState(!mClosed, "DataWriter is closed while writing chunks.");
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
      mStream.cancel();
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
        mStream.close();
        // Waiting for server to ack the close request
        Preconditions.checkState(mStream.receive(WRITE_TIMEOUT_MS) == null);
      }
    });
    mCloser.close();
  }

  /**
   * Creates an instance of {@link LocalFileDataWriter}.
   *
   * @param packetSize the packet size
   * @param blockWorker the block worker
   * @param writer the file writer
   * @param createRequest the request
   * @param stream the gRPC stream
   * @param closer the closer
   */
  private LocalFileDataWriter(long packetSize,
      BlockWorkerClient blockWorker, LocalFileBlockWriter writer,
      CreateLocalBlockRequest createRequest,
      GrpcBlockingStream<CreateLocalBlockRequest, CreateLocalBlockResponse> stream,
      Closer closer) {
    mBlockWorker = blockWorker;
    mCloser = closer;
    mWriter = writer;
    mCreateRequest = createRequest;
    mStream = stream;
    mPosReserved += FILE_BUFFER_BYTES;
    mChunkSize = packetSize;
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
    CreateLocalBlockRequest request = mCreateRequest.toBuilder().setSpaceToReserve(toReserve)
        .setOnlyReserveSpace(true).build();
    mStream.send(request, WRITE_TIMEOUT_MS);
    CreateLocalBlockResponse response = mStream.receive(WRITE_TIMEOUT_MS);
    Preconditions.checkState(response != null,
        String.format("Stream closed while waiting for reserve request %s", request.toString()));
    Preconditions.checkState(!response.hasPath(),
        String.format("Invalid response for reserve request %s", request.toString()));
    mPosReserved += toReserve;
  }
}

