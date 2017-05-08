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

import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic implementation of {@link UfsManager}.
 */
public abstract class AbstractUfsManager implements UfsManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractUfsManager.class);

  /**
   * The key of the UFS cache.
   */
  public static class Key {
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
          .add("properties", mProperties)
          .toString();
    }
  }

  /**
   * Maps from key to {@link UfsCounter} instances.
   * This map keeps the entire set of UFS instances and its counter,
   * each keyed by their unique combination of Uri and conf information.
   * This maps helps efficiently identify if a UFS instance in request
   * should be created or can be reused.
   */
  private final ConcurrentHashMap<Key, UfsCounter> mUnderFileSystemMap =
      new ConcurrentHashMap<>();
  /**
   * Maps from mount id to {@link UfsCounter} instances. This map helps efficiently retrieve
   * an existing UFS instance given its mount id.
   */
  private final ConcurrentHashMap<Long, UfsCounter> mMountIdToUnderFileSystemMap =
      new ConcurrentHashMap<>();

  private UnderFileSystem mRootUfs;
  protected final Closer mCloser;

  AbstractUfsManager() {
    mCloser = Closer.create();
  }

  /**
   * Establishes the connection to the given UFS from the server.
   *
   * @param ufs UFS instance
   * @throws IOException if failed to create the UFS instance
   */
  protected abstract void connect(UnderFileSystem ufs) throws IOException;

  /**
   * Return a UFS instance if it already exists in the cache, otherwise, creates a new instance and
   * return this.
   * @param ufsUri the UFS path
   * @param ufsConf the UFS configuration
   * @return the UFS instance and its reference counter
   * @throws IOException if it is failed to create the UFS instance
   */
  private UfsCounter getOrAdd(String ufsUri, Map<String, String> ufsConf)
      throws IOException {
    Key key = new Key(new AlluxioURI(ufsUri), ufsConf);
    UfsCounter cachedFs = mUnderFileSystemMap.get(key);
    if (cachedFs != null) {
      // It is a get operation, increase reference count.
      cachedFs.increaseReferenceAndGet();
      return cachedFs;
    }
    UnderFileSystem fs = UnderFileSystem.Factory.create(ufsUri, ufsConf);
    try {
      connect(fs);
    } catch (IOException e) {
      fs.close();
      throw e;
    }
    cachedFs = mUnderFileSystemMap.putIfAbsent(key, new UfsCounter(fs));
    if (cachedFs == null) {
      // above insert is successful
      // For a new created UnderFileSystem, its reference count is 1 already.
      UfsCounter ufsCounter = mUnderFileSystemMap.get(key);
      mCloser.register(ufsCounter.getUfs());
      return ufsCounter;
    }
    try {
      fs.close();
    } catch (IOException e) {
      // Cannot close the created ufs which fails the race.
      LOG.error("Failed to close UFS {}", fs, e);
    }
    return cachedFs;
  }

  @Override
  public UnderFileSystem addMount(long mountId, String ufsUri, Map<String, String> ufsConf)
      throws IOException {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    Preconditions.checkArgument(ufsUri != null, "uri");
    UfsCounter ufsCounter = getOrAdd(ufsUri, ufsConf);
    mMountIdToUnderFileSystemMap.put(mountId, ufsCounter);
    return ufsCounter.getUfs();
  }

  @Override
  public void removeMount(long mountId) {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    UfsCounter ufsCounter = mMountIdToUnderFileSystemMap.get(mountId);
    // Remove ufs from mMountIdToUnderFileSystemMap if its counter comes to zero.
    if (ufsCounter.decreaseReferenceAndGet() == 0L) {
      mMountIdToUnderFileSystemMap.remove(mountId);
    }
  }

  @Override
  public UnderFileSystem get(long mountId) {
    UfsCounter ufsCounter = mMountIdToUnderFileSystemMap.get(mountId);
    ufsCounter.increaseReferenceAndGet();
    return ufsCounter.getUfs();
  }

  @Override
  public UnderFileSystem getRoot() {
    synchronized (this) {
      if (mRootUfs == null) {
        String rootUri = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
        Map<String, String> rootConf =
            Configuration.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
        try {
          mRootUfs = addMount(IdUtils.ROOT_MOUNT_ID, rootUri, rootConf);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return mRootUfs;
    }
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  private static class UfsCounter {
    private UnderFileSystem mUfs;
    private final AtomicLong mCounter;

    UfsCounter(UnderFileSystem ufs) {
      mUfs = ufs;
      mCounter = new AtomicLong(1L);
    }

    UnderFileSystem getUfs() {
      return mUfs;
    }

    long increaseReferenceAndGet() {
      return mCounter.incrementAndGet();
    }

    long decreaseReferenceAndGet() {
      return mCounter.decrementAndGet();
    }
  }
}
