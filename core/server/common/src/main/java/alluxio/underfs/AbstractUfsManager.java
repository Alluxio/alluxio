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
import alluxio.concurrent.ManagedBlockingUfsForwarder;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.util.IdUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Basic implementation of {@link UfsManager}. Store the journal UFS and root
 * mount point information.
 */
public abstract class AbstractUfsManager implements UfsManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractUfsManager.class);

  private final Object mLock = new Object();

  /**
   * The key of the UFS cache.
   */
  public static class Key {
    private final String mScheme;
    private final String mAuthority;
    private final Map<String, String> mProperties;

    Key(AlluxioURI uri, Map<String, String> properties) {
      mScheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
      mAuthority = uri.getAuthority().toString().toLowerCase();
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
      return MoreObjects.toStringHelper(this)
          .add("authority", mAuthority)
          .add("scheme", mScheme)
          .add("properties", mProperties)
          .toString();
    }
  }

  // TODO(binfan): Add refcount to the UFS instance. Once the refcount goes to zero,
  // we could close this UFS instance.
  /**
   * Maps from key to {@link UnderFileSystem} instances. This map keeps the entire set of UFS
   * instances, each keyed by their unique combination of Uri and conf information. This map
   * helps efficiently identify if a UFS instance in request should be created or can be reused.
   */
  protected final ConcurrentHashMap<Key, UnderFileSystem> mUnderFileSystemMap =
      new ConcurrentHashMap<>();
  /**
   * Maps from mount id to {@link UfsClient} instances. This map helps efficiently retrieve
   * existing UFS info given its mount id.
   */
  private final ConcurrentHashMap<Long, UfsClient> mMountIdToUfsInfoMap =
      new ConcurrentHashMap<>();

  private UfsClient mRootUfsClient;
  private UfsClient mJournalUfsClient;
  protected final Closer mCloser;

  protected AbstractUfsManager() {
    mCloser = Closer.create();
  }

  /**
   * Return a UFS instance if it already exists in the cache, otherwise, creates a new instance and
   * return it.
   *
   * @param ufsUri the UFS path
   * @param ufsConf the UFS configuration
   * @return the UFS instance
   */
  private UnderFileSystem getOrAdd(AlluxioURI ufsUri, UnderFileSystemConfiguration ufsConf) {
    Key key = new Key(ufsUri, ufsConf.getMountSpecificConf());
    UnderFileSystem cachedFs = mUnderFileSystemMap.get(key);
    if (cachedFs != null) {
      return cachedFs;
    }
    // On cache miss, synchronize the creation to ensure ufs is only created once
    synchronized (mLock) {
      cachedFs = mUnderFileSystemMap.get(key);
      if (cachedFs != null) {
        return cachedFs;
      }
      UnderFileSystem fs = UnderFileSystem.Factory.create(ufsUri.toString(), ufsConf);

      // Detect whether to use managed blocking on UFS operations.
      boolean useManagedBlocking = fs.isObjectStorage();
      if (ufsConf.isSet(PropertyKey.MASTER_UFS_MANAGED_BLOCKING_ENABLED)) {
        useManagedBlocking = ufsConf.getBoolean(PropertyKey.MASTER_UFS_MANAGED_BLOCKING_ENABLED);
      }
      // Wrap UFS under managed blocking forwarder if required.
      if (useManagedBlocking) {
        fs = new ManagedBlockingUfsForwarder(fs);
      }

      if (mUnderFileSystemMap.putIfAbsent(key, fs) != null) {
        // This shouldn't occur unless our synchronization is incorrect
        LOG.warn("UFS already existed in UFS manager");
      }
      mCloser.register(fs);
      try {
        connectUfs(fs);
      } catch (IOException e) {
        LOG.warn("Failed to perform initial connect to UFS {}: {}", ufsUri, e.toString());
      }
      return fs;
    }
  }

  /**
   * Takes any necessary actions required to establish a connection to the under file system.
   * The implementation will either call {@link UnderFileSystem#connectFromMaster(String)} or
   *  {@link UnderFileSystem#connectFromWorker(String)} depending on the running process.
   */
  protected abstract void connectUfs(UnderFileSystem fs) throws IOException;

  @Override
  public void addMount(long mountId, final AlluxioURI ufsUri,
      final UnderFileSystemConfiguration ufsConf) {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    Preconditions.checkNotNull(ufsUri, "ufsUri");
    Preconditions.checkNotNull(ufsConf, "ufsConf");
    mMountIdToUfsInfoMap.put(mountId, new UfsClient(() -> getOrAdd(ufsUri, ufsConf), ufsUri));
  }

  @Override
  public void removeMount(long mountId) {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    // TODO(binfan): check the refcount of this ufs in mUnderFileSystemMap and remove it if this is
    // no more used. Currently, it is possibly used by out mount too.
    mMountIdToUfsInfoMap.remove(mountId);
  }

  @Override
  public UfsClient get(long mountId) throws NotFoundException, UnavailableException {
    UfsClient ufsClient = mMountIdToUfsInfoMap.get(mountId);
    if (ufsClient == null) {
      throw new NotFoundException(
          String.format("Mount Id %d not found in cached mount points", mountId));
    }
    return ufsClient;
  }

  @Override
  public UfsClient getRoot() {
    synchronized (this) {
      if (mRootUfsClient == null) {
        String rootUri = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
        boolean rootReadOnly =
            ServerConfiguration.getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY);
        boolean rootShared = ServerConfiguration
            .getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_SHARED);
        Map<String, String> rootConf =
            ServerConfiguration.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
        addMount(IdUtils.ROOT_MOUNT_ID, new AlluxioURI(rootUri),
            UnderFileSystemConfiguration.defaults(ServerConfiguration.global())
                .setReadOnly(rootReadOnly).setShared(rootShared).createMountSpecificConf(rootConf));
        try {
          mRootUfsClient = get(IdUtils.ROOT_MOUNT_ID);
        } catch (NotFoundException | UnavailableException e) {
          throw new RuntimeException("We should never reach here", e);
        }
      }
      return mRootUfsClient;
    }
  }

  @Override
  public UfsClient getJournal(URI location) {
    synchronized (this) {
      if (mJournalUfsClient == null) {
        addMount(IdUtils.UFS_JOURNAL_MOUNT_ID, new AlluxioURI(location.toString()),
            UfsJournal.getJournalUfsConf());
        try {
          mJournalUfsClient = get(IdUtils.UFS_JOURNAL_MOUNT_ID);
        } catch (NotFoundException | UnavailableException e) {
          throw new RuntimeException("We should never reach here", e);
        }
      }
      return mJournalUfsClient;
    }
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
