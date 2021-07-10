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

import alluxio.annotation.PublicApi;
import alluxio.grpc.TtlAction;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Wrapper around {@link FileInfo}. Represents the metadata about a file or directory in Alluxio.
 * This is a snapshot of information about the file or directory and not all attributes are
 * guaranteed to be up to date. Attributes documented as immutable will always be accurate, and
 * attributes documented as mutable may be out of date.
 */
@PublicApi
@ThreadSafe
public class URIStatus {
  /** Information about this URI retrieved from Alluxio master. */
  private final FileInfo mInfo;
  /** Context associated with this URI, possibly set by other external engines (e.g., presto). */
  private final CacheContext mCacheContext;

  /**
   * Constructs an instance of this class from a {@link FileInfo}.
   *
   * @param info an object containing the information about a particular uri
   */
  public URIStatus(FileInfo info) {
    this(info, null);
  }

  /**
   * Constructs an instance of this class from a {@link FileInfo}.
   *
   * @param info an object containing the information about a particular uri
   * @param context cache context associated with this uri
   */
  public URIStatus(FileInfo info, @Nullable CacheContext context) {
    mInfo = Preconditions.checkNotNull(info, "info");
    mCacheContext = context;
  }

  /**
   * @return the ACL entries for this path, mutable
   */
  public AccessControlList getAcl() {
    return mInfo.getAcl();
  }

  /**
   * @return the default ACL entries for this path, mutable
   */
  public DefaultAccessControlList getDefaultAcl() {
    return mInfo.getDefaultAcl();
  }

  /**
   * @param blockId the block ID
   * @return the corresponding block info or null
   */
  @Nullable
  public BlockInfo getBlockInfo(long blockId) {
    FileBlockInfo info = mInfo.getFileBlockInfo(blockId);
    return info == null ? null : info.getBlockInfo();
  }

  /**
   * @return a list of block ids belonging to the file, empty for directories, immutable
   */
  public List<Long> getBlockIds() {
    return mInfo.getBlockIds();
  }

  /**
   * @return the default block size for this file, 0 for directories, immutable
   */
  public long getBlockSizeBytes() {
    return mInfo.getBlockSizeBytes();
  }

  /**
   * @return the epoch time the entity referenced by this uri was created, immutable
   */
  public long getCreationTimeMs() {
    return mInfo.getCreationTimeMs();
  }

  /**
   * @return the unique long identifier of the entity referenced by this uri used by Alluxio
   *         servers, immutable
   */
  public long getFileId() {
    return mInfo.getFileId();
  }

  /**
   * @return the group that owns the entity referenced by this uri, mutable
   */
  public String getGroup() {
    return mInfo.getGroup();
  }

  /**
   * @return the percentage of blocks in Alluxio memory tier storage, mutable
   */
  public int getInMemoryPercentage() {
    return mInfo.getInMemoryPercentage();
  }

  /**
   * @return the percentage of blocks in Alluxio tier storage, mutable
   */
  public int getInAlluxioPercentage() {
    return mInfo.getInAlluxioPercentage();
  }

  /**
   * @return the epoch time the entity referenced by this uri was last modified, mutable
   */
  public long getLastModificationTimeMs() {
    return mInfo.getLastModificationTimeMs();
  }

  /**
   * @return the epoch time the entity referenced by this uri was last accessed, mutable
   */
  public long getLastAccessTimeMs() {
    return mInfo.getLastAccessTimeMs();
  }

  /**
   * @return the length in bytes of the file, 0 for directories, mutable
   */
  public long getLength() {
    return mInfo.getLength();
  }

  /**
   * For example for the uri: alluxio://host:1000/foo/bar/baz, baz is the name.
   *
   * @return the last path component of the entity referenced by this uri, mutable
   */
  public String getName() {
    return mInfo.getName();
  }

  /**
   * For example, for the uri: alluxio://host:1000/foo/bar/baz, the path is /foo/bar/baz.
   *
   * @return the entire path component of the entity referenced by this uri, mutable
   */
  public String getPath() {
    return mInfo.getPath();
  }

  /**
   * @return the int representation of the ACL mode bits of the entity referenced by this uri,
   *         mutable
   */
  public int getMode() {
    return mInfo.getMode();
  }

  /**
   * @return the string representation of the persistence status, mutable
   */
  // TODO(calvin): Consider returning the enum if it is moved to common
  public String getPersistenceState() {
    return mInfo.getPersistenceState();
  }

  /**
   * @return the time-to-live in milliseconds since the creation time of the entity referenced by
   *         this uri, mutable
   */
  public long getTtl() {
    return mInfo.getTtl();
  }

  /**
   * @return the action to perform on ttl expiry
   */
  public TtlAction getTtlAction() {
    return mInfo.getTtlAction();
  }

  /**
   * @return the uri of the under storage location of the entity referenced by this uri, mutable
   */
  public String getUfsPath() {
    return mInfo.getUfsPath();
  }

  /**
   * @return the owner of the entity referenced by this uri, mutable
   */
  public String getOwner() {
    return mInfo.getOwner();
  }

  /**
   * @return the maximum number of replicas of the entity referenced by this uri, mutable
   */
  public int getReplicationMax() {
    return mInfo.getReplicationMax();
  }

  /**
   * @return the minimum number of replicas of the entity referenced by this uri, mutable
   */
  public int getReplicationMin() {
    return mInfo.getReplicationMin();
  }

  /**
   * @return whether the entity referenced by this uri can be stored in Alluxio space, mutable
   */
  public boolean isCacheable() {
    return mInfo.isCacheable();
  }

  /**
   * @return whether the entity referenced by this uri has been marked as completed, immutable
   */
  public boolean isCompleted() {
    return mInfo.isCompleted();
  }

  /**
   * @return whether the entity referenced by this uri is a directory, immutable
   */
  // TODO(calvin): Consider consolidating the terms directory and folder
  public boolean isFolder() {
    return mInfo.isFolder();
  }

  /**
   * @return whether the entity referenced by this uri is persisted to an underlying storage,
   *         mutable
   */
  public boolean isPersisted() {
    return mInfo.isPersisted();
  }

  /**
   * @return whether the entity referenced by this uri is pinned, mutable
   */
  public boolean isPinned() {
    return mInfo.isPinned();
  }

  /**
   * @return the pinned location list
   */
  public Set<String> getPinnedMediumTypes() {
    return mInfo.getMediumTypes();
  }

  /**
   * @return whether the entity referenced by this uri is a mount point
   */
  public boolean isMountPoint() {
    return mInfo.isMountPoint();
  }

  /**
   * @return the id of the mount of this file is mapped to
   */
  public long getMountId() {
    return mInfo.getMountId();
  }

  /**
   * @return the list of file block descriptors
   */
  public List<FileBlockInfo> getFileBlockInfos() {
    return mInfo.getFileBlockInfos();
  }

  /**
   * @return the ufs fingerprint
   */
  public String getUfsFingerprint() {
    return mInfo.getUfsFingerprint();
  }

  /**
   * @return the extended attributes
   */
  public Map<String, byte[]> getXAttr() {
    return mInfo.getXAttr();
  }

  /**
   * @return the Presto context
   */
  @Nullable
  public CacheContext getCacheContext() {
    return mCacheContext;
  }

  /**
   * This is an experimental API. The returned {@link FileInfo} object does not have a stable API.
   * Make modifications to the returned file info object at your own risk.
   *
   * @return the underlying file info object
   */
  public FileInfo getFileInfo() {
    return mInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    URIStatus uriStatus = (URIStatus) o;
    return Objects.equals(mInfo, uriStatus.mInfo) && Objects
        .equals(mCacheContext, uriStatus.mCacheContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mInfo, mCacheContext);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("info", mInfo)
        .add("cacheContext", mCacheContext)
        .toString();
  }
}
