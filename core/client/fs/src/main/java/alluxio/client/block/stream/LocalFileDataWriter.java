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

import alluxio.client.WriteType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local data writer that simply writes packets to a local file.
 */
@NotThreadSafe
public final class LocalFileDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileDataWriter.class);

  private final long mFileBufferBytes;
  private final long mDataTimeoutMs;
  private final LocalFileBlockWriter mWriter;
  private final long mChunkSize;
  private final CreateLocalBlockRequest mCreateRequest;
  private final Closer mCloser;
  private final GrpcBlockingStream<CreateLocalBlockRequest, CreateLocalBlockResponse> mStream;

  /** The position to write the next byte at. */
  private long mPos;
  /** The number of bytes reserved on the block worker to hold the block. */
  private long mPosReserved;

  /**
   * Creates an instance of {@link LocalFileDataWriter}. This requires the block to be locked
   * beforehand.
   *
   * @param context the file system context
   * @param address the worker network address
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param options the output stream options
   * @return the {@link LocalFileDataWriter} created
   */
  public static LocalFileDataWriter create(final FileSystemContext context,
      final WorkerNetAddress address,
      long blockId, long blockSize, OutStreamOptions options) throws IOException {
    AlluxioConfiguration conf = context.getClusterConf();
    long chunkSize = conf.getBytes(PropertyKey.USER_LOCAL_WRITER_CHUNK_SIZE_BYTES);

    Closer closer = Closer.create();
    try {
      CloseableResource<BlockWorkerClient> blockWorker =
          context.acquireBlockWorkerClient(address);
      closer.register(blockWorker);
      int writerBufferSizeMessages =
          conf.getInt(PropertyKey.USER_STREAMING_WRITER_BUFFER_SIZE_MESSAGES);
      long fileBufferBytes = conf.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
      long dataTimeout = conf.getMs(PropertyKey.USER_STREAMING_DATA_WRITE_TIMEOUT);
      // in cases we know precise block size, make more accurate reservation.
      long reservedBytes = Math.min(blockSize, conf.getBytes(PropertyKey.USER_FILE_RESERVED_BYTES));

      CreateLocalBlockRequest.Builder builder =
          CreateLocalBlockRequest.newBuilder().setBlockId(blockId).setTier(options.getWriteTier())
              .setSpaceToReserve(reservedBytes).setMediumType(options.getMediumType())
              .setPinOnCreate(options.getWriteType() == WriteType.ASYNC_THROUGH);
      if (options.getWriteType() == WriteType.ASYNC_THROUGH
          && conf.getBoolean(PropertyKey.USER_FILE_UFS_TIER_ENABLED)) {
        builder.setCleanupOnFailure(false);
      }
      CreateLocalBlockRequest createRequest = builder.build();

      GrpcBlockingStream<CreateLocalBlockRequest, CreateLocalBlockResponse> stream =
          new GrpcBlockingStream<>(blockWorker.get()::createLocalBlock, writerBufferSizeMessages,
              MoreObjects.toStringHelper(LocalFileDataWriter.class)
                  .add("request", createRequest)
                  .add("address", address)
                  .toString());
      stream.send(createRequest, dataTimeout);
      CreateLocalBlockResponse response = stream.receive(dataTimeout);
      Preconditions.checkState(response != null && response.hasPath());
      LocalFileBlockWriter writer =
          closer.register(new LocalFileBlockWriter(response.getPath()));
      return new LocalFileDataWriter(chunkSize, writer, createRequest, stream, closer,
          fileBufferBytes, dataTimeout);
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
      Preconditions.checkState(!mStream.isCanceled() && !mStream.isClosed(),
          "DataWriter is closed while writing chunks.");
      int sz = buf.readableBytes();
      ensureReserved(mPos + sz);
      mPos += sz;
      Preconditions.checkState(mWriter.append(buf) == sz);
      MetricsSystem.counter(MetricKey.CLIENT_BYTES_WRITTEN_LOCAL.getName()).inc(sz);
      MetricsSystem.meter(MetricKey.CLIENT_BYTES_WRITTEN_LOCAL_THROUGHPUT.getName()).mark(sz);
    } finally {
      buf.release();
    }
  }

  @Override
  public void cancel() throws IOException {
    mCloser.register(() -> mStream.cancel());
    mCloser.close();
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    mCloser.register(() -> {
      mStream.close();
      mStream.waitForComplete(mDataTimeoutMs);
    });
    mCloser.close();
  }

  /**
   * Creates an instance of {@link LocalFileDataWriter}.
   *
   * @param packetSize the packet size
   * @param writer the file writer
   * @param createRequest the request
   * @param stream the gRPC stream
   * @param closer the closer
   */
  private LocalFileDataWriter(long packetSize, LocalFileBlockWriter writer,
      CreateLocalBlockRequest createRequest,
      GrpcBlockingStream<CreateLocalBlockRequest, CreateLocalBlockResponse> stream,
      Closer closer, long fileBufferBytes, long dataTimeoutMs) {
    mFileBufferBytes = fileBufferBytes;
    mDataTimeoutMs = dataTimeoutMs;
    mCloser = closer;
    mWriter = writer;
    mCreateRequest = createRequest;
    mStream = stream;
    mPosReserved = createRequest.getSpaceToReserve();
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
    long toReserve = Math.max(pos - mPosReserved, mFileBufferBytes);
    CreateLocalBlockRequest request =
        mCreateRequest.toBuilder().setSpaceToReserve(toReserve).setOnlyReserveSpace(true).build();
    mStream.send(request, mDataTimeoutMs);
    CreateLocalBlockResponse response = mStream.receive(mDataTimeoutMs);
    Preconditions.checkState(response != null,
        String.format("Stream closed while waiting for reserve request %s", request.toString()));
    Preconditions.checkState(!response.hasPath(),
        String.format("Invalid response for reserve request %s", request.toString()));
    mPosReserved += toReserve;
  }
}
