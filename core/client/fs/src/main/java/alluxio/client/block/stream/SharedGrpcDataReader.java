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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.Math;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A shared gRPC data reader that cache blocks data for multi-thread accessing.
 *
 * It follows GrpcDataReader protocol and takes strong assumption:
 * Parallel read to the same file happens on the same time, so that read request is
 * serialized by kernel
 */
@NotThreadSafe
public class SharedGrpcDataReader implements DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(SharedGrpcDataReader.class);
  private static final int BLOCK_LOCK_NUM = 32;
  //
  // BLOCK_LOCKS is used to ensure thread-safety when referencing or updating the shared data
  // reader for block i in different FileInStream instances.
  // BLOCK_READERS is a ConcurrentHashMap from block id to its shared data reader, as different
  // DataReader may be needed to handle different blocks at the same time.
  //
  /** An array of locks to guard cached data readers based on block id. */
  private static final ReentrantReadWriteLock[] BLOCK_LOCKS =
      new ReentrantReadWriteLock[BLOCK_LOCK_NUM];
  /** A map from block id to the block's cached data reader. */
  private static final ConcurrentHashMap<Long, BufferCachingGrpcDataReader> BLOCK_READERS =
      new ConcurrentHashMap<>();
  /** A hashing function to map block id to one of the locks. */
  private static final HashFunction HASH_FUNC = Hashing.murmur3_32();

  static {
    for (int i = 0; i < BLOCK_LOCK_NUM; i++) {
      BLOCK_LOCKS[i] = new ReentrantReadWriteLock();
    }
  }

  private static ReentrantReadWriteLock getLock(long blockId) {
    return BLOCK_LOCKS[Math.floorMod(HASH_FUNC.hashLong(blockId).asInt(), BLOCK_LOCKS.length)];
  }

  private final long mBlockId;
  private final BufferCachingGrpcDataReader mCachedDataReader;
  private final long mChunkSize;

  /** The next pos to read. */
  private long mPosToRead;

  /**
   * Creates an instance of {@link SharedGrpcDataReader}.
   *
   * @param readRequest the read request
   * @param reader the cached Grpc data reader for the given block
   */
  @VisibleForTesting
  protected SharedGrpcDataReader(ReadRequest readRequest, BufferCachingGrpcDataReader reader) {
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
   * Seeks to a specific position.
   *
   * @param pos the position to seek to
   */
  public void seek(long pos) {
    mPosToRead = pos;
  }

  @Override
  @Nullable
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
  public void close() throws IOException {
    if (mCachedDataReader.deRef() > 0) {
      return;
    }
    try (LockResource lockResource = new LockResource(getLock(mBlockId).writeLock())) {
      if (mCachedDataReader.getRefCount() == 0) {
        BLOCK_READERS.remove(mBlockId);
        mCachedDataReader.close();
      }
    }
  }

  /**
   * Factory class to create {@link SharedGrpcDataReader}s.
   */
  public static class Factory implements DataReader.Factory {
    private final FileSystemContext mContext;
    private final WorkerNetAddress mAddress;
    private final ReadRequest.Builder mReadRequestBuilder;
    private final long mBlockSize;

    /**
     * Creates an instance of {@link SharedGrpcDataReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param readRequestBuilder the builder of read request
     * @param blockSize the block size
     */
    public Factory(FileSystemContext context, WorkerNetAddress address,
        ReadRequest.Builder readRequestBuilder, long blockSize) {
      mContext = context;
      mAddress = address;
      mReadRequestBuilder = readRequestBuilder;
      mBlockSize = blockSize;
    }

    @alluxio.annotation.SuppressFBWarnings(
        value = "AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
        justification = "operation is still atomic guarded by block Ølock")
    @Override
    public DataReader create(long offset, long len) throws IOException {
      long blockId = mReadRequestBuilder.getBlockId();
      BufferCachingGrpcDataReader reader;
      try (LockResource lockResource = new LockResource(getLock(blockId).writeLock())) {
        reader = BLOCK_READERS.get(blockId);
        if (reader == null) {
          // Even we may only need a portion, create a reader to read the whole block
          ReadRequest cacheRequest = mReadRequestBuilder.setOffset(0).setLength(mBlockSize).build();
          reader = BufferCachingGrpcDataReader.create(mContext, mAddress, cacheRequest);
          BLOCK_READERS.put(blockId, reader);
        }

        reader.ref();
      }
      return new SharedGrpcDataReader(
          mReadRequestBuilder.setOffset(offset).setLength(len).build(), reader);
    }

    @Override
    public void close() throws IOException {}
  }
}

