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

import alluxio.Sessions;
import alluxio.client.keyvalue.ByteBufferKeyValuePartitionReader;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *  A pool to store ByteBufferKeyValuePartitionReader,BlockReader and the associated lockId.This
 *  pool has a fixed size and uses the access order to evict when the pool is full.
 *  ???a function needs to be implemented:how to close this pool and how to fix when master call
 *  deleteStore
 */
public class ByteBufferKeyValuePartitionReaderPool {
  private final int mSize;
  /** queue of blockIds for evict. */
  private final Queue<Long> mReaderQueue = new LinkedList<>();
  /** a map to store the ByteBufferKeyValuePartitionReader,BlockReader and the lockId. */
  private final ConcurrentMap<Long, Future<ReaderRecord>> mReaderMap = new ConcurrentHashMap<>();
  /** BlockWorker handler for access block info. */
  private final BlockWorker mBlockWorker;

  /** */
  private final Lock mLock = new ReentrantLock();
  /** If the eldest reader in the pool is in use, use this condition to wait.*/
  private final Condition mWaitOnEldestReaderRecord = mLock.newCondition();

  /**
   *Constructs a {@link ByteBufferKeyValuePartitionReaderPool} instance.
   *
   * @param size size of the pool
   * @param blockWorker the blockworker to use to unlock a block
   */
  public ByteBufferKeyValuePartitionReaderPool(int size, BlockWorker blockWorker) {
    mSize = size;
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
  }

  //Called by getReader() when there is no matching reader in the pool to the requested blockId.
  private ReaderRecord createReaderRecord(long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    final long sessionId = Sessions.KEYVALUE_SESSION_ID;
    final long lockId = mBlockWorker.lockBlock(sessionId, blockId);
    BlockReader blockReader = mBlockWorker.readBlockRemote(sessionId, blockId, lockId);
    ByteBuffer fileBuffer = blockReader.read(0, blockReader.getLength());
    ByteBufferKeyValuePartitionReader reader = new ByteBufferKeyValuePartitionReader(fileBuffer);
    // TODO(binfan): clean fileBuffer which is a direct byte buffer

    return new ReaderRecord(reader, blockReader, lockId);
  }

  /**
   * If the requested ByteBufferKeyValuePartitionReader is in the pool, return it from the pool.
   * Otherwise call {@link #createReaderRecord(long)} and put the ReaderRecord in the pool.Use
   * {@link FutureTask} to guarente that the ReaderRecord can only be created once.
   *
   * @param blockId the requested blockId
   * @return the the requested ByteBufferKeyValuePartitionReader
   * @throws InvalidWorkerStateException if reaching an invalid state
   * @throws BlockDoesNotExistException  if the worker is not serving this block
   * @throws IOException if read operation failed
   */
  ByteBufferKeyValuePartitionReader getReader(final Long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    while (true) {
      Future<ReaderRecord> readerRecordFuture = mReaderMap.get(blockId);
      if (readerRecordFuture == null || isFullEldest(blockId)) {
        Callable<ReaderRecord> readerRecordTask = new Callable<ReaderRecord>() {
          public ReaderRecord call()
              throws InterruptedException, BlockDoesNotExistException, IOException,
              InvalidWorkerStateException {
            return createReaderRecord(blockId);
          }
        };
        FutureTask<ReaderRecord> readerRecordFutureTask = new FutureTask<>(readerRecordTask);
        readerRecordFuture = put(blockId, readerRecordFutureTask);
        if (readerRecordFuture == null) {
          readerRecordFuture = readerRecordFutureTask;
          readerRecordFutureTask.run();
        }
      }
      try {
        ReaderRecord readerRecord = readerRecordFuture.get();
        readerRecord.mRefCnt.incrementAndGet();
        return readerRecord.getByteBufferKeyValuePartitionReader();
      } catch (InterruptedException | ExecutionException e) {
        mReaderMap.remove(blockId, readerRecordFuture);
      }
    }
  }

  /**
   * if the specified blockId is already with a ReaderRecord, return the existing one.Otherwise
   * associate it with the given ReaderRecord.
   *
   * @param blockId the blockId with which the ReaderRecord is associated
   * @param readerRecordFuture the entry to be ReaderRecord
   * @return the previous value associated with the specified key,
   *         or {@code null} if there was no mapping for the key
   * @throws IOException if read operation failed
   * @throws BlockDoesNotExistException if the worker is not serving this block
   */
  private Future<ReaderRecord> put(final Long blockId,
      final Future<ReaderRecord> readerRecordFuture)
      throws IOException, BlockDoesNotExistException {
    mLock.lock();
    try {
      if (mReaderQueue.contains(blockId) && !isFullEldest(blockId)) {
      //change the position of an exsiting key to the tail of the queue.
        mReaderQueue.remove(blockId);
        mReaderQueue.add(blockId);
        return mReaderMap.get(blockId);
      }
      if (mReaderQueue.size() > mSize) {
        Long eldestBlockId = mReaderQueue.peek();
        ReaderRecord eldestReaderRecord = null;
        try {
          eldestReaderRecord = mReaderMap.get(eldestBlockId).get();
        } catch (InterruptedException e) {
          mReaderMap.remove(eldestBlockId);
        } catch (ExecutionException e) {
          throw Throwables.propagate(e);
        }
        if (eldestReaderRecord.mRefCnt.get() == 0) {
          removeEldestReaderRecord();
        } else {
          while (eldestReaderRecord.mRefCnt.get() > 0) {
            mWaitOnEldestReaderRecord.await();
          }
        }
      }
      mReaderQueue.add(blockId);
      return mReaderMap.put(blockId, readerRecordFuture);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      mLock.unlock();
    }
  }

  /**
   * This must be called when the user finished using the ByteBufferKeyValuePartitionReader.When the
   * pool is full, it also needs to remove the eldest ReaderRecord.
   * @param blockId the block id
   * @throws IOException if read operation failed
   * @throws BlockDoesNotExistException if the worker is not serving this block
   */
  public void releaseReader(Long blockId) throws IOException, BlockDoesNotExistException {
    ReaderRecord readerRecord = null;
    try {
      readerRecord = mReaderMap.get(blockId).get();
    } catch (InterruptedException e) {
      mReaderMap.remove(blockId);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
    readerRecord.mRefCnt.decrementAndGet();
    mLock.lock();
    try {
      if (isFullEldest(blockId) && readerRecord.mRefCnt.get() == 0) {
        removeEldestReaderRecord();
        mWaitOnEldestReaderRecord.signalAll();
      }
    } finally {
      mLock.unlock();
    }
  }

  private void removeEldestReaderRecord()
      throws IOException, BlockDoesNotExistException {
    Long eldestBlockId = mReaderQueue.poll();
    closeReaderRecord(eldestBlockId);
    mReaderMap.remove(eldestBlockId);
  }

  /**
   * Release the resource of an ReaderRecord given an blockId. It closes
   * the ByteBufferKeyValuePartitionReader and BlockReader, then unlock the associated block by
   * lockId.
   *
   * @param blockId the blockId
   * @throws IOException if read operation failed
   * @throws BlockDoesNotExistException if the worker is not serving this block
   */
  private void closeReaderRecord(Long blockId) throws IOException, BlockDoesNotExistException {
    ReaderRecord eldestReaderRecord = null;
    try {
      eldestReaderRecord = mReaderMap.get(blockId).get();
    } catch (InterruptedException e) {
      mReaderMap.remove(blockId);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
    eldestReaderRecord.getByteBufferKeyValuePartitionReader().close();
    eldestReaderRecord.mBlockReader.close();
    mBlockWorker.unlockBlock(eldestReaderRecord.mLockId);
  }

  private boolean isFullEldest(Long blockId) {
    return mReaderQueue.size() > mSize && blockId.equals(mReaderQueue.peek());
  }

  /**
   * ByteBufferKeyValuePartitionReader pool's record, the lock is also in the entry.
   */
  private static final class ReaderRecord {
    private final ByteBufferKeyValuePartitionReader mByteBufferKeyValuePartitionReader;
    private final BlockReader mBlockReader;
    private final long mLockId;
    /** the number of users on this record.*/
    private final AtomicInteger mRefCnt;

    public ReaderRecord(ByteBufferKeyValuePartitionReader byteBufferKeyValuePartitionReader,
        BlockReader blockReader, long lockId) {
      mByteBufferKeyValuePartitionReader = byteBufferKeyValuePartitionReader;
      mBlockReader = blockReader;
      mLockId = lockId;
      mRefCnt = new AtomicInteger(0);
    }

    public ByteBufferKeyValuePartitionReader getByteBufferKeyValuePartitionReader() {
      return mByteBufferKeyValuePartitionReader;
    }
  }
}

