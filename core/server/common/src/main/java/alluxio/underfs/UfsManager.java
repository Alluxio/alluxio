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
import alluxio.util.IdUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class manages the UFS used by different worker services.
 */
@ThreadSafe
public class UfsManager implements Closeable {

  /**
   * The key of the UFS cache.
   */
  private static class Key {
    private final String mScheme;
    private final String mAuthority;
    private final Map<String, String> mProperties;

    Key(AlluxioURI uri, Map<String, String> properties) {
      mScheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
      mAuthority = uri.getAuthority() == null ? "" : uri.getAuthority().toLowerCase();
      mProperties = (properties == null || properties.isEmpty()) ? null : properties;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mScheme, mAuthority, mProperties);
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
      return Objects.equal(mAuthority, that.mAuthority) && Objects
          .equal(mProperties, that.mProperties) && Objects.equal(mScheme, that.mScheme);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("authority", mAuthority).add("scheme", mScheme)
          .add("property", mProperties).toString();
    }
  }

  /** Maps from key to {@link UnderFileSystem} instances. */
  private final ConcurrentHashMap<Key, UnderFileSystem> mUnderFileSystemMap =
      new ConcurrentHashMap<>();
  /** Maps from mount id to {@link UnderFileSystem} instances. */
  private final ConcurrentHashMap<Long, UnderFileSystem> mMountIdToUnderFileSystemMap =
      new ConcurrentHashMap<>();

  private UnderFileSystem mRootUfs = null;
  protected final Closer mCloser;

  /**
   * Constructs the instance.
   */
  public UfsManager() {
    mCloser = Closer.create();
  }

  /**
   * Gets a UFS instance from the cache if exists. Otherwise, creates a new instance and adds
   * that to the cache. Use this method only when you create new UFS instances.
   *
   * @param path the UFS path
   * @param ufsConf the UFS configuration
   * @return the UFS instance
   */
  public UnderFileSystem getOrCreate(String path, Map<String, String> ufsConf) {
    Key key = new Key(new AlluxioURI(path), ufsConf);
    UnderFileSystem cachedFs = mUnderFileSystemMap.get(key);
    if (cachedFs != null) {
      return cachedFs;
    }
    UnderFileSystem fs = UnderFileSystemRegistry.create(path, ufsConf);
    UnderFileSystem racingFs = mUnderFileSystemMap.putIfAbsent(key, fs);
    if (racingFs == null) {
      mCloser.register(fs);
      return fs;
    }
    try {
      fs.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return racingFs;
  }

  /**
   * Marks a UFS instance as a mount entry and associates it with a mount id.
   *
   * @param ufs the UFS
   * @param mountId the mount id
   */
  public void addMount(UnderFileSystem ufs, long mountId) {
    Preconditions.checkArgument(ufs != null, "ufs");
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    mMountIdToUnderFileSystemMap.put(mountId, ufs);
  }

  /**
   * Marks a UFS instance as root.
   *
   * @param ufs the UFS
   */
  public void addRoot(UnderFileSystem ufs) {
    Preconditions.checkArgument(ufs != null, "ufs");
    mRootUfs = ufs;
  }

  /**
   * Gets a UFS instance from the cache if exists, or null otherwise.
   *
   * @param mountId the mount id
   * @return the UFS instance
   */
  public UnderFileSystem getByMountId(long mountId) {
    return mMountIdToUnderFileSystemMap.get(mountId);
  }

  /**
   * @return the UFS instance associated with root
   */
  public UnderFileSystem getRoot() {
    return mRootUfs;
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
