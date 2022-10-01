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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.collections.LockPool;
import alluxio.concurrent.LockMode;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.resource.LockResource;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for managing metadata sync locking.
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
          ServerConfiguration.getInt(PropertyKey.MASTER_METADATA_SYNC_LOCK_POOL_INITSIZE),
          ServerConfiguration.getInt(PropertyKey.MASTER_METADATA_SYNC_LOCK_POOL_LOW_WATERMARK),
          ServerConfiguration.getInt(PropertyKey.MASTER_METADATA_SYNC_LOCK_POOL_HIGH_WATERMARK),
          ServerConfiguration.getInt(PropertyKey.MASTER_METADATA_SYNC_LOCK_POOL_CONCURRENCY_LEVEL));

  /**
   * Acquire locks for a given path before metadata sync.
   * Locks are for leading paths of a uri instead of inodes
   * and hence paths will be locked regardless if they exist in alluxio or not.
   * All locks but the last one will be read locks and the last lock acquired will be a write lock.
   *
   * For example, for path /a/b/c/d, this method will acquire the following locks sequentially
   * 1. a read lock on /
   * 2. a read lock on /a/
   * 3. a read lock on /a/b/
   * 4. a read lock on /a/b/c/
   * 5. a write lock on /a/b/c/d/
   *
   * @param uri the alluxio uri to perform metadata sync
   * @return the {@link MetadataSyncPathList} representing the locked paths
   */
  public MetadataSyncPathList lockPath(AlluxioURI uri) throws InvalidPathException, IOException {
    MetadataSyncPathList list = new MetadataSyncPathList();
    String[] components = PathUtils.getPathComponents(uri.getPath());
    StringBuilder sb = new StringBuilder();
    // TODO(elega) potential inefficient string operations
    try {
      for (int i = 0; i < components.length; ++i) {
        sb.append(components[i]);
        sb.append(AlluxioURI.SEPARATOR);
        String lockKey = sb.toString();
        Preconditions.checkNotNull(lockKey);
        LockMode lockMode = (i == components.length - 1) ? LockMode.WRITE : LockMode.READ;
        list.add(mLockPool.get(lockKey, lockMode));
      }
    } catch (Throwable t) {
      list.close();
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
  @NotThreadSafe
  public static class MetadataSyncPathList implements Closeable {
    List<LockResource> mLockResources = new ArrayList<>();

    private void add(LockResource lockResource) {
      mLockResources.add(lockResource);
    }

    @Override
    public void close() throws IOException {
      for (int i = mLockResources.size() - 1; i >= 0; --i) {
        mLockResources.get(i).close();
      }
    }
  }
}
