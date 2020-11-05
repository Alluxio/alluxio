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
import alluxio.grpc.ReadRequest;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.resource.LockResource;
import alluxio.wire.WorkerNetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A shared gRPC data reader that cache blocks data for multi-thread accessing.
 *
 * It follows GrpcDataReader protocol and takes strong assumption:
 * Parallel read to the same file happens on the same time, so that read request is
 * serialized by kernel
 */
@NotThreadSafe
public final class NaiveSharedGrpcDataReader implements DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(NaiveSharedGrpcDataReader.class);

  private static final ReentrantReadWriteLock BLOCK_LOCK = new ReentrantReadWriteLock();
  // A map from block id to the block's cached data reader
  @GuardedBy("mBlockLocks")
  private static final Map<Long, NaiveCachedGrpcDataReader> BLOCK_READERS = new HashMap<>();

  private final NaiveCachedGrpcDataReader mCachedDataReader;
  private final long mBlockId;
  private long mChunkSize;
  /** The next pos to read. */
  private long mPosToRead;

  /**
   * Creates an instance of {@link NaiveSharedGrpcDataReader}.
   *
   * @param readRequest the read request
   * @param reader the cached Grpc data reader for the given block
   */
  private NaiveSharedGrpcDataReader(ReadRequest readRequest, NaiveCachedGrpcDataReader reader) {
    mChunkSize = readRequest.getChunkSize();
    mPosToRead = readRequest.getOffset();
    mBlockId = readRequest.getBlockId();
    mCachedDataReader = reader;
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  /**
   * Seek to a specific position.
   *
   * @param pos the position to seek to
   */
  public void seek(long pos) {
    mPosToRead = pos;
  }

  @Override
  public DataBuffer readChunk() throws IOException {
    int index = (int) (mPosToRead / mChunkSize);
    DataBuffer chunk = mCachedDataReader.readChunk(index);
    if (chunk == null) {
      return null;
    }
    ByteBuffer bb = chunk.getReadOnlyByteBuffer();
    // Force to align to chunk size
    bb.position((int) (mPosToRead % mChunkSize));
    mPosToRead += mChunkSize - mPosToRead % mChunkSize;

    return new NioDataBuffer(bb, bb.remaining());
  }

  @Override
  public DataBuffer readChunkIfReady() throws IOException {
    // I'm naive, I'm reading chunks anyway
    return readChunk();
  }

  @Override
  public void close() throws IOException {
    if (mCachedDataReader.deRef() == 0) {
      try (LockResource lockResource = new LockResource(BLOCK_LOCK.writeLock())) {
        if (mCachedDataReader.getRefCount() == 0) {
          mCachedDataReader.close();
          BLOCK_READERS.remove(mBlockId);
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
    private final long mBlockSize;

    /**
     * Creates an instance of {@link NaiveSharedGrpcDataReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param readRequestPartial the partial read request
     * @param blockSize the block size
     */
    public Factory(FileSystemContext context, WorkerNetAddress address,
        ReadRequest readRequestPartial, long blockSize) {
      mContext = context;
      mAddress = address;
      mReadRequestPartial = readRequestPartial;
      mBlockSize = blockSize;
    }

    @Override
    public DataReader create(long offset, long len) throws IOException {
      long blockId = mReadRequestPartial.getBlockId();
      NaiveCachedGrpcDataReader reader;
      try (LockResource lockResource = new LockResource(BLOCK_LOCK.writeLock())) {
        reader = BLOCK_READERS.get(blockId);
        if (reader == null) {
          // I'm naive, I always read from 0 and read the whole block
          ReadRequest cacheRequest = mReadRequestPartial
              .toBuilder().setOffset(0).setLength(mBlockSize).build();
          reader = new NaiveCachedGrpcDataReader
              .Factory(mContext, mAddress, cacheRequest).create();
          BLOCK_READERS.put(blockId, reader);
        }

        reader.ref();
      }
      return new NaiveSharedGrpcDataReader(mReadRequestPartial
          .toBuilder().setOffset(offset).setLength(len).build(), reader);
    }

    @Override
    public boolean isShortCircuit() {
      return false;
    }

    @Override
    public void close() throws IOException {}
  }
}

