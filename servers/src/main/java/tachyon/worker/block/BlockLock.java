package tachyon.worker.block;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A ReadWrite Lock to guard one block. There should be only one lock per block.
 */
public class BlockLock implements ReadWriteLock {
  public enum BlockLockType {
    READ,  // A read lock
    WRITE,  // A write lock
  }

  private final ReentrantReadWriteLock mLock;
  /** The block Id this lock guards **/
  private final long mBlockId;


  public BlockLock(long blockId) {
    mBlockId = blockId;
    mLock = new ReentrantReadWriteLock();
  }

  public long getBlockId() {
    return mBlockId;
  }

  @Override
  public Lock readLock() {
    return mLock.readLock();
  }

  @Override
  public Lock writeLock() {
    return mLock.writeLock();
  }
}
