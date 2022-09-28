package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.collections.LockPool;
import alluxio.concurrent.LockMode;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.resource.LockResource;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Class for managing metadata　sync locking.
 */
public class MetadataSyncLockManager {
  /**
   * Pool for supplying metadata sync locks.
   *
   * We use weak values so that when nothing holds a reference to
   * a lock, the garbage collector can remove the lock's entry from the pool.
   */
  private final LockPool<String> mLockPool =
      new LockPool<>((key) -> new ReentrantReadWriteLock(),
          Configuration.getInt(PropertyKey.MASTER_LOCK_POOL_INITSIZE),
          Configuration.getInt(PropertyKey.MASTER_LOCK_POOL_LOW_WATERMARK),
          Configuration.getInt(PropertyKey.MASTER_LOCK_POOL_HIGH_WATERMARK),
          Configuration.getInt(PropertyKey.MASTER_LOCK_POOL_CONCURRENCY_LEVEL));

  /**
   * Acquire locks for a given path before metadata sync.
   * Locks are for leading paths of a uri instead of inodes
   * and hence paths will be locked regardless if they exist in alluxio or not.
   * All locks but the last one will be read locks and the last lock acquired will be a write lock.
   *
   * For example, for path /a/b/c/d, this method will acquire the following locks sequentially
   * 1. a read lock on /
   * 2. a read lock on /a
   * 3. a read lock on /a/b
   * 4. a read lock on /a/b/c
   * 5. a write lock on /a/b/c/d
   *
   * @param uri the alluxio uri to perform metadata sync
   * @return the {@link MetadataSyncPathList} representing the locked paths
   */
  public MetadataSyncPathList lockPath(AlluxioURI uri) {
    MetadataSyncPathList list = new MetadataSyncPathList();
    int depth = uri.getDepth();
    // TODO(elega) potential inefficient string operations
    for (int i = 0; i <= depth; ++i) {
      String lockKey = uri.getLeadingPath(i);
      Preconditions.checkNotNull(lockKey);
      LockMode lockMode = (i == depth) ? LockMode.WRITE : LockMode.READ;
      list.add(mLockPool.get(lockKey, lockMode));
    }
    return list;
  }

  /**
   * @return the size of the lock pool
   */
  public int getLockPoolSize() {
    return mLockPool.size();
  }

  /**
   * Represents a locked path for a metadata sync path.
   */
  public static class MetadataSyncPathList implements Closeable {
    List<LockResource> mLockResources = new ArrayList<>();

    private synchronized void add(LockResource lockResource) {
      mLockResources.add(lockResource);
    }

    @Override
    public synchronized void close() throws IOException {
      for (int i = mLockResources.size() - 1; i >= 0; --i) {
        mLockResources.get(i).close();
      }
    }
  }
}
