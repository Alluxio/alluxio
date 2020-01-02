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

package alluxio.client.file;

import alluxio.conf.AlluxioConfiguration;
import alluxio.master.MasterInquireClient;
import alluxio.uri.Authority;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A cache for storing {@link FileSystem} clients. This should only be used by the Factory class.
 */
@ThreadSafe
public class FileSystemCache {
  private final Object mLock = new Object();

  @GuardedBy("mLock")
  private final HashMap<Key, Value> mCacheMap = new HashMap<>();

  /**
   * Constructs a new cache for file system instances.
   */
  public FileSystemCache() { }

  /**
   * Gets a {@link FileSystem} instance from the cache. If there is none,
   * a new instance is created and inserted into the cache.
   * Note that, the returned instance will be a wrapper of the cached instance which
   * has its own close state.
   *
   * @param key the key to retrieve a {@link FileSystem}
   * @return the {@link FileSystem} associated with the key
   */
  public FileSystem get(Key key) {
    synchronized (mLock) {
      Value value = mCacheMap.get(key);
      FileSystem fs;
      if (value == null) {
        // In case cache miss, create a new FileSystem instance,
        // which will decrement the ref count on close;
        fs = FileSystem.Factory.create(FileSystemContext.create(key.mSubject, key.mConf));
        mCacheMap.put(key, new Value(fs));
      } else {
        fs = value.mFileSystem;
        value.mRefCount.getAndIncrement();
      }
      return new InstanceCachingFileSystem(fs, key);
    }
  }

  /**
   * Closes and removes all {@link FileSystem} from the cache. Only to be used for testing
   * purposes.
   */
  @VisibleForTesting
  void purge() {
    synchronized (mLock) {
      new HashSet<>(mCacheMap.values()).forEach(value -> {
        try {
          value.mRefCount.set(1);
          value.mFileSystem.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  /**
   * A key which can be used to look up a {@link FileSystem} instance in the
   * {@link FileSystemCache}.
   */
  public static class Key {
    final Subject mSubject;
    final Authority mAuth;

    /**
     * Only used to store the configuration. Allows us to compute a {@link FileSystem} directly
     * from a key.
     */
    final AlluxioConfiguration mConf;

    /**
     * @param subject Subject of the user
     * @param conf Alluxio configuration
     */
    public Key(Subject subject, AlluxioConfiguration conf) {
      mConf = conf;
      mSubject = subject;
      mAuth = MasterInquireClient.Factory.getConnectDetails(conf).toAuthority();
    }

    @Override
    public int hashCode() {
      return Objects.hash(mSubject, mAuth);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Key)) {
        return false;
      }
      Key otherKey = (Key) o;
      return Objects.equals(mSubject, otherKey.mSubject)
          && Objects.equals(mAuth, otherKey.mAuth);
    }
  }

  /**
   * A value wraps a {@link FileSystem} instance and a ref count in the {@link FileSystemCache}.
   */
  public static class Value {
    final FileSystem mFileSystem;
    final AtomicInteger mRefCount;

    /**
     * @param fileSystem filesystem instance cached
     */
    public Value(FileSystem fileSystem) {
      this.mFileSystem = fileSystem;
      this.mRefCount = new AtomicInteger(1);
    }
  }

  /**
   * A wrapper class on a FileSystem instance. On Close, it will decrement the refcount of
   * the underlying File System instance in Cache. If this ref count becomes
   * zero, the underlying cached instance will be removed from the cache.
   */
  public class InstanceCachingFileSystem extends DelegatingFileSystem {
    final Key mKey;
    boolean mClosed = false;

    /**
     * Wraps a file system instance to cache.
     *
     * @param fs fs instance
     * @param key key in fs instance cache
     */
    InstanceCachingFileSystem(FileSystem fs, Key key) {
      super(fs);
      mKey = key;
    }

    @Override
    public boolean isClosed() {
      return mClosed;
    }

    @Override
    public void close() throws IOException {
      if (!mClosed) {
        mClosed = true;
        // Decrement the ref count. If the ref count goes zero, remove the entry from cache
        synchronized (FileSystemCache.this.mLock) {
          Value value = mCacheMap.get(mKey);
          Preconditions.checkNotNull(value);
          if (value.mRefCount.decrementAndGet() == 0) {
            mCacheMap.remove(mKey);
            super.close();
          }
        }
      }
    }
  }
}
