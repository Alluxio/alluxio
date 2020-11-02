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

import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.DataMessage;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.resource.CloseableResource;
import alluxio.resource.LockResource;
import alluxio.wire.WorkerNetAddress;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A gRPC data reader that streams a region from gRPC data server.
 *
 * NaiveSharedGrpcDataReader is a POC to solve multi-process-read perf issue.
 * It follows GrpcDataReader protocol.
 * It takes strong assumption:
 *    Parallel read to the same file happens on the same time, so that read request is 
 *    serialized by kernel
 */
@NotThreadSafe
public final class NaiveSharedGrpcDataReader implements DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(NaiveSharedGrpcDataReader.class);

  private final NaiveCachedGrpcDataReader mCachedDataReader;
  private final long mBlockId;

  private long mChunkSize;
  private long mBlockSize;

  /** The next pos to read. */
  private long mPosToRead;

  private final static ReentrantReadWriteLock mBlockLocks = new ReentrantReadWriteLock();
  private final static Map<Long, NaiveCachedGrpcDataReader> mBlockReaders = new HashMap<>();

  /**
   * Creates an instance of {@link NaiveSharedGrpcDataReader}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param readRequest the read request
   */
  private NaiveSharedGrpcDataReader(FileSystemContext context, WorkerNetAddress address,
      ReadRequest readRequest) throws IOException {
    mChunkSize = readRequest.getChunkSize();
    mPosToRead = readRequest.getOffset();
    mBlockSize = readRequest.getLength() + readRequest.getOffset();
    try (LockResource r1 = new LockResource(mBlockLocks.writeLock())) {
      mBlockId = readRequest.getBlockId();
      NaiveCachedGrpcDataReader reader = mBlockReaders.get(mBlockId);
      if (reader == null) {
        // I'm naive, I always read from 0 and read the whole block
        ReadRequest cacheRequest = readRequest.toBuilder().setOffset(0).setLength(readRequest.getOffset()+ readRequest.getLength()).build();
        reader = new NaiveCachedGrpcDataReader(context, address, cacheRequest);
        mBlockReaders.put(mBlockId, reader);
      }

      reader.ref();
      mCachedDataReader = reader;
    }
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  public void seek(long pos) {
    mPosToRead = pos;
  }

  @Override
  public DataBuffer readChunk() throws IOException {
    int index = (int)(mPosToRead / mChunkSize);
    DataBuffer chunk = mCachedDataReader.readChunk(index);
    if (chunk == null) {
      return null;
    }
    ByteBuffer bb = chunk.getReadOnlyByteBuffer();
    // Force to align to chunk size
    bb.position((int)(mPosToRead % mChunkSize));
    mPosToRead += mChunkSize - mPosToRead % mChunkSize;

    return new NioDataBuffer(bb, bb.remaining());
  }

  @Override
  public DataBuffer readChunkNoWait() throws IOException {
    // I'm naive, I'm reading chunks anyway
    return readChunk();
  }

  @Override
  public void close() throws IOException {
    if (mCachedDataReader.deRef() == 0) {
      try (LockResource r1 = new LockResource(mBlockLocks.writeLock())) {
        if (mCachedDataReader.getRefCount() == 0) {
          mCachedDataReader.close();
          mBlockReaders.remove(mBlockId);
        }
      }
    }
  }

  /**
   * Factory class to create {@link NaiveSharedGrpcDataReader}s.
   */
  public static class Factory implements DataReader.Factory {
    private final FileSystemContext mContext;
    private final WorkerNetAddress mAddress;
    private final ReadRequest mReadRequestPartial;

    /**
     * Creates an instance of {@link NaiveSharedGrpcDataReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param readRequestPartial the partial read request
     */
    public Factory(FileSystemContext context, WorkerNetAddress address,
        ReadRequest readRequestPartial) {
      mContext = context;
      mAddress = address;
      mReadRequestPartial = readRequestPartial;
    }

    @Override
    public DataReader create(long offset, long len) throws IOException {
      return new NaiveSharedGrpcDataReader(mContext, mAddress,
          mReadRequestPartial.toBuilder().setOffset(offset).setLength(len).build());
    }

    @Override
    public boolean isShortCircuit() {
      return false;
    }

    @Override
    public void close() throws IOException {}
  }
}

