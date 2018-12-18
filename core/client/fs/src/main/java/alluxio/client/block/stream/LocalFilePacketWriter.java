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
import alluxio.exception.status.CanceledException;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.grpc.Context;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local packet writer that simply writes packets to a local file.
 */
@NotThreadSafe
public final class LocalFilePacketWriter implements PacketWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFilePacketWriter.class);
  private static final long FILE_BUFFER_BYTES =
      Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);

  private final BlockWorkerClient mBlockWorker;
  private final LocalFileBlockWriter mWriter;
  private final long mPacketSize;
  private final CreateLocalBlockRequest mCreateRequest;
  private final OutStreamOptions mOptions;
  private final Closer mCloser;
  private final Context.CancellableContext mCancellableContext;

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
      Context.CancellableContext cancellableContext = Context.current().withCancellation();
      Context previousContext = cancellableContext.attach();
      CreateLocalBlockResponse response;
      try {
        response = blockWorker.createLocalBlock(createRequest);
      } finally {
        cancellableContext.detach(previousContext);
      }
      LocalFileBlockWriter writer =
          closer.register(new LocalFileBlockWriter(response.getPath()));
      return new LocalFilePacketWriter(packetSize, options, blockWorker,
          writer, createRequest, cancellableContext, closer);
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
      mCancellableContext.cancel(new CanceledException("Operation canceled by client"));
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
      public void close() {
        mCancellableContext.close();
      }
    });
    mCloser.close();
  }

  /**
   * Creates an instance of {@link LocalFilePacketWriter}.
   * @param packetSize the packet size
   * @param options the output stream options
   * @param cancellableContext
   */
  private LocalFilePacketWriter(long packetSize, OutStreamOptions options,
      BlockWorkerClient blockWorker, LocalFileBlockWriter writer,
      CreateLocalBlockRequest createRequest, Context.CancellableContext cancellableContext,
      Closer closer) {
    mBlockWorker = blockWorker;
    mCloser = closer;
    mOptions = options;
    mWriter = writer;
    mCreateRequest = createRequest;
    mCancellableContext = cancellableContext;
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
    mBlockWorker.createLocalBlock(mCreateRequest.toBuilder().setSpaceToReserve(toReserve)
          .setOnlyReserveSpace(true).build());
    mPosReserved += toReserve;
  }
}

