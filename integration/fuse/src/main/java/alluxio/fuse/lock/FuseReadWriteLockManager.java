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

package alluxio.fuse.lock;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

import alluxio.Constants;
import alluxio.concurrent.ClientRWLock;
import alluxio.concurrent.LockMode;
import alluxio.exception.runtime.CancelledRuntimeException;
import alluxio.exception.runtime.DeadlineExceededRuntimeException;
import alluxio.resource.CloseableResource;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * The lock manager to guard Fuse read/write operations.
 */
public class FuseReadWriteLockManager {
  private static final long TRY_LOCK_TIMEOUT = 20 * Constants.SECOND_MS;
  // Maximum readers allowed for each file
  private static final int MAX_READER_CONCURRENCY = 64;

  private final LoadingCache<String, ClientRWLock> mLockCache
      = CacheBuilder.newBuilder().weakValues()
      .build(new CacheLoader<String, ClientRWLock>() {
        @Override
        public ClientRWLock load(String key) {
          return new ClientRWLock(MAX_READER_CONCURRENCY);
        }
      });

  /**
   * Constructs a new {@link FuseReadWriteLockManager}.
   */
  public FuseReadWriteLockManager() {}

  /**
   * Tries to lock the given poth with read/write mode.
   *
   * @param path the path to lock
   * @param mode the lock mode
   * @return the lock resource to unlock the locked lock
   */
  public CloseableResource<Lock> tryLock(String path, LockMode mode) {
    ClientRWLock pathLock = mLockCache.getUnchecked(getHashedKey(path));
    Lock lock = mode == LockMode.READ ? pathLock.readLock() : pathLock.writeLock();
    try {
      if (!lock.tryLock(TRY_LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
        throw new DeadlineExceededRuntimeException(String.format(
            "Failed to acquire lock for path %s after %s ms "
                + "(LockMode: %s, lock reference count = %s)",
            path, TRY_LOCK_TIMEOUT, mode, pathLock.getReferenceCount()));
      }
      return new CloseableResource<Lock>(lock) {
        @Override
        public void closeResource() {
          lock.unlock();
        }
      };
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancelledRuntimeException(String.format(
          "Failed to acquire lock for path %s after %s ms: interrupted "
              + "(LockMode: %s, lock reference count = %s)",
          path, TRY_LOCK_TIMEOUT, mode, pathLock.getReferenceCount()));
    }
  }

  private String getHashedKey(String key) {
    // File path is a unique identifier for a file, however it can be a long string
    // hence using md5 hash of the file path as the lock identifier
    return md5().hashString(key, UTF_8).toString();
  }
}
