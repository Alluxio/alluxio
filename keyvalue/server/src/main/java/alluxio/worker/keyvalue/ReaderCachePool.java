/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.keyvalue;

import alluxio.Constants;
import alluxio.Sessions;
import alluxio.client.keyvalue.ByteBufferKeyValuePartitionReader;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 *  A pool to store ByteBufferKeyValuePartitionReader,BlockReader and the associated lockId.This
 *  pool has a fixed size and uses the access order to evict when the pool is full.
 *  To avoid some entries resident in the pool for a long time, use a Evict Thread to evict
 *  the eldest entry once in a while.
 */
public class ReaderCachePool {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final int mSize;
  /** queue of blockIds for evict. */
  private Queue<Long> mReaderQueue = new LinkedList<>();
  /** a map to store the ByteBufferKeyValuePartitionReader,BlockReader and the lockId. */
  private ConcurrentMap<Long, Future<CacheEntry>> mReaderMap = new ConcurrentHashMap<>();
    /** BlockWorker handler for access block info. */
  private final BlockWorker mBlockWorker;

  /**
   * Reader pool's entry, the lock is also in the entry.
   */
  static class CacheEntry {
    private ByteBufferKeyValuePartitionReader mByteBufferKeyValuePartitionReader;
    private BlockReader mBlockReader;
    private long mLockId;

    public CacheEntry(ByteBufferKeyValuePartitionReader byteBufferKeyValuePartitionReader,
        BlockReader blockReader, long lockId) {
      mByteBufferKeyValuePartitionReader = byteBufferKeyValuePartitionReader;
      mBlockReader = blockReader;
      mLockId = lockId;
    }

    public ByteBufferKeyValuePartitionReader getByteBufferKeyValuePartitionReader() {
      return mByteBufferKeyValuePartitionReader;
    }

    public BlockReader getBlockReader() {
      return mBlockReader;
    }

    public long getLockId() {
      return mLockId;
    }
  }

  /**
   *Constructs a {@link ReaderCachePool} instance.
   *
   * @param size size of the pool
   * @param blockWorker the blockworker to use to unlock a block
   */
  public ReaderCachePool(int size, BlockWorker blockWorker) {
    mSize = size;
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    // launch the evict thread.
    Executors.newSingleThreadExecutor().submit(new EvictThread());
  }

  //Called by #getReader() when there is no matching readers to the blockId.
  private CacheEntry createCacheEntry(long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    final long sessionId = Sessions.KEYVALUE_SESSION_ID;
    final long lockId = mBlockWorker.lockBlock(sessionId, blockId);
    BlockReader blockReader = mBlockWorker.readBlockRemote(sessionId, blockId, lockId);
    ByteBuffer fileBuffer = blockReader.read(0, blockReader.getLength());
    ByteBufferKeyValuePartitionReader reader = new ByteBufferKeyValuePartitionReader(fileBuffer);
    // TODO(binfan): clean fileBuffer which is a direct byte buffer

    return new CacheEntry(reader, blockReader, lockId);
  }

  /**
   * If the requested ByteBufferKeyValuePartitionReader is in the pool, return it from the pool.
   * Otherwise call {@link #createCacheEntry(long)} and put the CacheEntry in the pool.
   *
   * @param blockId the requested blockId
   * @return
   * @throws InvalidWorkerStateException if reaching an invalid state
   * @throws BlockDoesNotExistException  if the worker is not serving this block
   * @throws IOException if read operation failed
   */
  ByteBufferKeyValuePartitionReader getReader(final Long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    while (true) {
      Future<CacheEntry> f = mReaderMap.get(blockId);
      if (f == null) {
        Callable<CacheEntry> eval = new Callable<CacheEntry>() {
          public CacheEntry call()
              throws InterruptedException, BlockDoesNotExistException, IOException,
              InvalidWorkerStateException {
            return createCacheEntry(blockId);
          }
        };
        FutureTask<CacheEntry> ft = new FutureTask<>(eval);
        f = putIfAbsent(blockId, ft);
        if (f == null) {
          f = ft;
          ft.run();
        }
      }
      try {
        return f.get().getByteBufferKeyValuePartitionReader();
      } catch (InterruptedException e) {
        mReaderMap.remove(blockId, f);
      } catch (ExecutionException e) {
        LOG.error("create CacheEntry failed", e);
      }
    }
  }

  /**
   * if the specified blockId is already with a CacheEntry, return the existing one.Otherwise
   * associate it with the given cacheEntry.
   *
   * @param blockId the blockId with which the cacheEntry is associated
   * @param cacheEntry the entry to be cached
   * @return the previous value associated with the specified key,
   *         or {@code null} if there was no mapping for the key
   * @throws IOException if read operation failed
   * @throws BlockDoesNotExistException if the worker is not serving this block
   */
  private synchronized Future<CacheEntry> putIfAbsent(final Long blockId,
      final Future<CacheEntry> cacheEntry) throws IOException, BlockDoesNotExistException {
    if (mReaderMap.containsKey(blockId)) {
      //change the position of an exsiting key to the tail of the queue.
      mReaderQueue.remove(blockId);
      mReaderQueue.add(blockId);
      return mReaderMap.get(blockId);
    }
    if (mReaderQueue.size() > mSize) {
      removeEldestCacheEntry();
    }
    mReaderQueue.add(blockId);
    return mReaderMap.put(blockId, cacheEntry);
  }

  /**
   * Release the resource of an cacheEntry given an blockId. It closes
   * the ByteBufferKeyValuePartitionReader and BlockReader, then unlock the associated block by
   * lockId.
   *
   * @param blockId the blockId
   * @throws IOException if read operation failed
   * @throws BlockDoesNotExistException if the worker is not serving this block
   */
  private void release(long blockId) throws IOException, BlockDoesNotExistException {
    CacheEntry eldestEntry = null;
    try {
      eldestEntry = mReaderMap.get(blockId).get();
    } catch (InterruptedException e) {
      mReaderMap.remove(blockId);
    } catch (ExecutionException e) {
      LOG.error("create CacheEntry failed", e);
    }
    eldestEntry.getByteBufferKeyValuePartitionReader().close();
    eldestEntry.getBlockReader().close();
    mBlockWorker.unlockBlock(eldestEntry.getLockId());
  }

  private synchronized void removeEldestCacheEntry()
      throws IOException, BlockDoesNotExistException {
    Long eldestBlockId = mReaderQueue.poll();
    release(eldestBlockId);
    mReaderMap.remove(eldestBlockId);
  }

  /**
   * This thread is always running to evict the eldest entry to avoid some entry resident
   * in the pool for too long.
   */
  final class EvictThread implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          long sleepSeconds = (long) (mSize - mReaderQueue.size()) * Constants.MINUTE_MS;
          CommonUtils.sleepMs(sleepSeconds);
          if (mReaderQueue.size() > 0) {
            removeEldestCacheEntry();
          }
        } catch (IOException e) {
          e.printStackTrace();
        } catch (BlockDoesNotExistException e) {
          e.printStackTrace();
        }
      }
    }
  }
}

