package alluxio.worker.block;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * A resource lock for block.
 */
public class BlockLock implements Closeable {

  private final long mLockId;
  private final Consumer<Long> mUnlock;

  /**
   * @param lockId lockId
   * @param unlock unlock function
   */
  public BlockLock(long lockId, Consumer<Long> unlock) {
    mLockId = lockId;
    mUnlock = unlock;
  }

  /**
   * @return lockId
   */
  public long getLockId() {
    return mLockId;
  }

  @Override
  public void close() {
    mUnlock.accept(mLockId);
  }
}
