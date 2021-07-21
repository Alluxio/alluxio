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

import alluxio.conf.AlluxioConfiguration;
import alluxio.client.ReadType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.conf.PropertyKey;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A data reader that simply reads packets from a local file.
 */
@NotThreadSafe
public final class LocalFileDataReader implements DataReader {
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
    MetricsSystem.counter(MetricKey.CLIENT_BYTES_READ_LOCAL.getName()).inc(dataBuffer.getLength());
    MetricsSystem.meter(MetricKey.CLIENT_BYTES_READ_LOCAL_THROUGHPUT.getName())
        .mark(dataBuffer.getLength());
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
    private final CloseableResource<BlockWorkerClient> mBlockWorker;
    private final long mBlockId;
    private final String mPath;
    private final long mLocalReaderChunkSize;
    private final int mReadBufferSize;
    private final GrpcBlockingStream<OpenLocalBlockRequest, OpenLocalBlockResponse> mStream;

    private LocalFileBlockReader mReader;
    private final long mDataTimeoutMs;
    private boolean mClosed;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param context the file system context
     * @param address the worker address
     * @param blockId the block ID
     * @param localReaderChunkSize chunk size in bytes for local reads
     * @param options the instream options
     */
    public Factory(FileSystemContext context, WorkerNetAddress address, long blockId,
        long localReaderChunkSize, InStreamOptions options) throws IOException {
      AlluxioConfiguration conf = context.getClusterConf();
      mBlockId = blockId;
      mLocalReaderChunkSize = localReaderChunkSize;
      mReadBufferSize = conf.getInt(PropertyKey.USER_STREAMING_READER_BUFFER_SIZE_MESSAGES);
      mDataTimeoutMs = conf.getMs(PropertyKey.USER_STREAMING_DATA_READ_TIMEOUT);
      if (conf.getBoolean(PropertyKey.USER_DIRECT_MEMORY_IO_ENABLED)) {
        mBlockWorker = null;
        mStream = null;
        PropertyKey tierDirPathConf =
            PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0);
        String storageDir = conf.get(tierDirPathConf).split(",")[0];
        String workerDir = conf.get(PropertyKey.WORKER_DATA_FOLDER);
        mPath = Paths.get(storageDir, workerDir, Long.toString(blockId)).toString();
        return;
      }

      boolean isPromote = ReadType.fromProto(options.getOptions().getReadType()).isPromote();
      OpenLocalBlockRequest request = OpenLocalBlockRequest.newBuilder()
          .setBlockId(mBlockId).setPromote(isPromote).build();

      mBlockWorker = context.acquireBlockWorkerClient(address);
      try {
        mStream = new GrpcBlockingStream<>(mBlockWorker.get()::openLocalBlock, mReadBufferSize,
            MoreObjects.toStringHelper(LocalFileDataReader.class)
                .add("request", request)
                .add("address", address)
                .toString());
        mStream.send(request, mDataTimeoutMs);
        OpenLocalBlockResponse response = mStream.receive(mDataTimeoutMs);
        Preconditions.checkState(response.hasPath());
        mPath = response.getPath();
      } catch (Exception e) {
        mBlockWorker.close();
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
      return new LocalFileDataReader(mReader, offset, len, mLocalReaderChunkSize);
    }

    @Override
    public void close() throws IOException {
      if (mClosed) {
        return;
      }
      try {
        if (mReader != null) {
          mReader.close();
        }
        if (mStream != null) {
          mStream.close();
          mStream.waitForComplete(mDataTimeoutMs);
        }
      } finally {
        mClosed = true;
        if (mBlockWorker != null) {
          mBlockWorker.close();
        }
      }
    }
  }
}

