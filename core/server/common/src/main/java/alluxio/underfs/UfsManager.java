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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.IdUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class manages the UFS used by different services.
 */
@ThreadSafe
public class UfsManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(UfsManager.class);
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
      return Objects.toStringHelper(this)
          .add("authority", mAuthority)
          .add("scheme", mScheme)
          .add("properties", mProperties).toString();
    }
  }

  // TODO(binfan): Add refcount to the UFS instance. Once the refcount goes to zero,
  // we could close this UFS instance.
  /**
   * Maps from key to {@link UnderFileSystem} instances. This map keeps the entire set of UFS
   * instances, each keyed by their unique combination of Uri and conf information. This map
   * helps efficiently  identify if a UFS instance in request should be created or can be reused.
   */
  private final ConcurrentHashMap<Key, UnderFileSystem> mUnderFileSystemMap =
      new ConcurrentHashMap<>();
  /**
   * Maps from mount id to {@link UnderFileSystem} instances. This map helps efficiently retrieve
   * a existing UFS instance given its mount id.
   */
  private final ConcurrentHashMap<Long, UnderFileSystem> mMountIdToUnderFileSystemMap =
      new ConcurrentHashMap<>();

  private final UnderFileSystem mRootUfs;
  protected final Closer mCloser;

  /**
   * Constructs the instance.
   */
  public UfsManager() {
    mCloser = Closer.create();
    String rootUri = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Map<String, String> rootConf =
        Configuration.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
    mRootUfs = UnderFileSystemRegistry.create(rootUri, rootConf);
    mUnderFileSystemMap.put(new Key(new AlluxioURI(rootUri), rootConf), mRootUfs);
    mMountIdToUnderFileSystemMap.put(IdUtils.ROOT_MOUNT_ID, mRootUfs);
    mCloser.register(mRootUfs);
  }

  /**
   * Return a UFS instance if it already exists in the cache, otherwise, creates a new instance and
   * return this.
   *
   * @param ufsUri the UFS path
   * @param ufsConf the UFS configuration
   * @return the UFS instance
   */
  private UnderFileSystem getOrAdd(String ufsUri, Map<String, String> ufsConf) {
    Key key = new Key(new AlluxioURI(ufsUri), ufsConf);
    UnderFileSystem cachedFs = mUnderFileSystemMap.get(key);
    if (cachedFs != null) {
      return cachedFs;
    }
    UnderFileSystem fs = UnderFileSystemRegistry.create(ufsUri, ufsConf);
    cachedFs = mUnderFileSystemMap.putIfAbsent(key, fs);
    if (cachedFs == null) {
      // above insert is successful
      mCloser.register(fs);
      return fs;
    }
    try {
      fs.close();
    } catch (IOException e) {
      // Cannot close the created ufs which fails the race.
      LOG.error("Failed to close ufs {}", fs, e);
    }
    return cachedFs;
  }

  /**
   * Maps a mount id to a UFS instance. Based on the UFS uri and conf, if this UFS instance already
   * exists in the cache, map the mount id to this existing instance. Otherwise, creates a new
   * instance and adds that to the cache. Use this method only when you create new UFS instances.
   *
   * @param mountId the mount id
   * @param ufsUri the UFS path
   * @param ufsConf the UFS configuration
   * @return the UFS instance
   */
  public UnderFileSystem addMount(long mountId, String ufsUri, Map<String, String> ufsConf) {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    Preconditions.checkArgument(ufsUri != null, "uri");
    UnderFileSystem ufs = getOrAdd(ufsUri, ufsConf);
    mMountIdToUnderFileSystemMap.put(mountId, ufs);
    return ufs;
  }

  /**
   * Marks a UFS instance as a mount entry and associates it with a mount id.
   *  @param mountId the mount id
   *
   */
  public void removeMount(long mountId) {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    mMountIdToUnderFileSystemMap.remove(mountId);
  }

  /**
   * Gets a UFS instance from the cache if exists, or null otherwise.
   *
   * @param mountId the mount id
   * @return the UFS instance
   */
  public UnderFileSystem get(long mountId) {
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
