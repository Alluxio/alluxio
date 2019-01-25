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
import alluxio.client.ReadType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A data reader that simply reads packets from a local file.
 */
@NotThreadSafe
public final class LocalFileDataReader implements DataReader {
  private static final int READ_BUFFER_SIZE =
      Configuration.getInt(PropertyKey.USER_NETWORK_READER_BUFFER_SIZE_MESSAGES);
  private static final long READ_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_DATA_TIMEOUT_MS);
  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;
  private final long mEnd;
  private final long mChunkSize;
  private long mPos;
  private boolean mClosed;

  /**
   * Creates an instance of {@link LocalFileDataReader}.
   *
   * @param reader the file reader to the block path
   * @param offset the offset
   * @param len the length to read
   * @param chunkSize the chunk size
   */
  private LocalFileDataReader(LocalFileBlockReader reader, long offset, long len, long chunkSize) {
    mReader = reader;
    Preconditions.checkArgument(chunkSize > 0);
    mPos = offset;
    mEnd = Math.min(mReader.getLength(), offset + len);
    mChunkSize = chunkSize;
  }

  @Override
  public DataBuffer readChunk() throws IOException {
    if (mPos >= mEnd) {
      return null;
    }
    ByteBuffer buffer = mReader.read(mPos, Math.min(mChunkSize, mEnd - mPos));
    DataBuffer dataBuffer = new NioDataBuffer(buffer, buffer.remaining());
    mPos += dataBuffer.getLength();
    MetricsSystem.counter(ClientMetrics.BYTES_READ_LOCAL).inc(dataBuffer.getLength());
    MetricsSystem.meter(ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT).mark(dataBuffer.getLength());
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mReader.decreaseUsageCount();
  }

  /**
   * Factory class to create {@link LocalFileDataReader}s.
   */
  @NotThreadSafe
  public static class Factory implements DataReader.Factory {
    private final BlockWorkerClient mBlockWorker;
    private final FileSystemContext mContext;
    private final WorkerNetAddress mAddress;
    private final long mBlockId;
    private final String mPath;
    private final long mChunkSize;
    private final GrpcBlockingStream<OpenLocalBlockRequest, OpenLocalBlockResponse> mStream;
    private LocalFileBlockReader mReader;
    private boolean mClosed;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param context the file system context
     * @param address the worker address
     * @param blockId the block ID
     * @param chunkSize the packet size
     * @param options the instream options
     */
    public Factory(FileSystemContext context, WorkerNetAddress address, long blockId,
        long chunkSize, InStreamOptions options) throws IOException {
      mContext = context;
      mAddress = address;
      mBlockId = blockId;
      mChunkSize = chunkSize;

      boolean isPromote = ReadType.fromProto(options.getOptions().getReadType()).isPromote();
      OpenLocalBlockRequest request = OpenLocalBlockRequest.newBuilder()
          .setBlockId(mBlockId).setPromote(isPromote).build();
      mBlockWorker = context.acquireBlockWorkerClient(address);
      try {
        mStream = new GrpcBlockingStream<>(mBlockWorker::openLocalBlock, READ_BUFFER_SIZE,
            address.toString());
        mStream.send(request, READ_TIMEOUT_MS);
        OpenLocalBlockResponse response = mStream.receive(READ_TIMEOUT_MS);
        Preconditions.checkState(response.hasPath());
        mPath = response.getPath();
      } catch (Exception e) {
        context.releaseBlockWorkerClient(address, mBlockWorker);
        throw e;
      }
    }

    @Override
    public DataReader create(long offset, long len) throws IOException {
      if (mReader == null) {
        mReader = new LocalFileBlockReader(mPath);
      }
      Preconditions.checkState(mReader.getUsageCount() == 0);
      mReader.increaseUsageCount();
      return new LocalFileDataReader(mReader, offset, len, mChunkSize);
    }

    @Override
    public boolean isShortCircuit() {
      return true;
    }

    @Override
    public void close() throws IOException {
      if (mClosed) {
        return;
      }
      if (mReader != null) {
        mReader.close();
      }
      try {
        mStream.close();
        mStream.waitForComplete(READ_TIMEOUT_MS);
      } finally {
        mClosed = true;
        mContext.releaseBlockWorkerClient(mAddress, mBlockWorker);
      }
    }
  }
}

