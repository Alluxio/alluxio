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

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.master.MasterInquireClient;
import alluxio.uri.Authority;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A cache for storing {@link FileSystem} clients. This should only be used by the Factory class.
 */
@ThreadSafe
public class FileSystemCache {
  @GuardedBy("itself")
  private final HashMap<Key, InstanceCachingFileSystem> mCacheMap = new HashMap<>();

  /**
   * Constructs a new cache for file system instances.
   */
  public FileSystemCache() { }

  /**
   * Gets a {@link FileSystem} from the cache. If there is none, one is created, inserted into
   * the cache, and returned back to the user.
   *
   * @param key the key to retrieve a {@link FileSystem}
   * @return the {@link FileSystem} associated with the key
   */
  public FileSystem get(Key key) {
    synchronized (mCacheMap) {
      InstanceCachingFileSystem fs;
      if (mCacheMap.containsKey(key)) {
        fs = mCacheMap.get(key);
        fs.incrementCount();
      } else {
        // In case cache miss, create a new instance which will decrement the ref count on close;
        fs = new InstanceCachingFileSystem(
            FileSystem.Factory.create(FileSystemContext.create(key.mSubject, key.mConf)),
            this, key);
        mCacheMap.put(key, fs);
      }
      return fs;
    }
  }

  /**
   * Removes the client with the given key from the cache. Returns the client back to the user.
   *
   * @param key the client key to remove
   * @return The removed context or null if there is no client associated with the key
   */
  public FileSystem remove(Key key) {
    synchronized (mCacheMap) {
      return mCacheMap.remove(key);
    }
  }

  /**
   * Closes and removes all {@link FileSystem} from the cache. Only to be used for testing
   * purposes. This method operates on the assumption that no concurrent calls to get/remove
   * will be made while this function is running.
   */
  @VisibleForTesting
  void purge() {
    synchronized (mCacheMap) {
      mCacheMap.forEach((fsKey, fs) -> {
        try {
          mCacheMap.remove(fsKey);
          fs.close();
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

    /**
     * @param ctx client context
     */
    public Key(ClientContext ctx) {
      this(ctx.getSubject(), ctx.getClusterConf());
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
}
