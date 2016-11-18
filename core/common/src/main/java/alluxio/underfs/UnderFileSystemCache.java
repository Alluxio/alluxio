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

package alluxio.underfs;

import alluxio.AlluxioURI;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Cache for retrieving {@link UnderFileSystem}.
 */
@ThreadSafe
public final class UnderFileSystemCache {
  private static final Cache UFS_CACHE = new Cache();

  /**
   * A class used to cache UnderFileSystems.
   */
  @ThreadSafe
  private static class Cache {
    /**
     * Maps from {@link Key} to {@link UnderFileSystem} instances.
     */
    private final ConcurrentHashMap<Key, UnderFileSystem> mUnderFileSystemMap =
        new ConcurrentHashMap<>();

    Cache() {}

    /**
     * Gets a UFS instance from the cache if exists. Otherwise, creates a new instance and adds
     * that to the cache.
     *
     * @param path the ufs path
     * @param ufsConf the ufs configuration
     * @return the UFS instance
     */
    UnderFileSystem get(String path, Object ufsConf) {
      Key key = new Key(new AlluxioURI(path));
      UnderFileSystem cachedFs = mUnderFileSystemMap.get(key);
      if (cachedFs != null) {
        return cachedFs;
      }
      UnderFileSystem fs = UnderFileSystemRegistry.create(path, ufsConf);
      cachedFs = mUnderFileSystemMap.putIfAbsent(key, fs);
      if (cachedFs == null) {
        return fs;
      }
      try {
        fs.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return cachedFs;
    }

    void clear() {
      mUnderFileSystemMap.clear();
    }
  }

  /**
   * The key of the UFS cache.
   */
  private static class Key {
    private final String mScheme;
    private final String mAuthority;

    Key(AlluxioURI uri) {
      mScheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
      mAuthority = uri.getAuthority() == null ? "" : uri.getAuthority().toLowerCase();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mScheme, mAuthority);
    }

    @Override
    public boolean equals(Object object) {
      if (object == this) {
        return true;
      }

      if (!(object instanceof Key)) {
        return false;
      }

      Key that = (Key) object;
      return Objects.equal(mScheme, that.mScheme)
          && Objects.equal(mAuthority, that.mAuthority);
    }

    @Override
    public String toString() {
      return mScheme + "://" + mAuthority;
    }
  }

  /**
   * Prevent instance creation.
   */
  private UnderFileSystemCache() {
  }

  /**
   * Clears the under file system cache.
   */
  public static void clearCache() {
    UFS_CACHE.clear();
  }

  /**
   * Gets the UnderFileSystem instance according to its schema.
   *
   * @param path the file path storing over the ufs
   * @return instance of the under layer file system
   */
  public static UnderFileSystem get(String path) {
    return get(path, null);
  }

  /**
   * Gets the UnderFileSystem instance according to its scheme and configuration.
   *
   * @param path the file path storing over the ufs
   * @param ufsConf the configuration object for ufs only
   * @return instance of the under layer file system
   */
  public static UnderFileSystem get(String path, Object ufsConf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    return UFS_CACHE.get(path, ufsConf);
  }
}
