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

package alluxio.wire;

import static alluxio.util.StreamUtils.map;

import alluxio.Constants;
import alluxio.security.authorization.AccessControlList;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The file information.
 */
@NotThreadSafe
// TODO(jiri): Consolidate with URIStatus.
public final class FileInfo implements Serializable {
  private static final long serialVersionUID = 7119966306934831779L;

  private long mFileId;
  private String mName = "";
  private String mPath = "";
  private String mUfsPath = "";
  private long mLength;
  private long mBlockSizeBytes;
  private long mCreationTimeMs;
  private boolean mCompleted;
  private boolean mFolder;
  private boolean mPinned;
  private boolean mCacheable;
  private boolean mPersisted;
  private ArrayList<Long> mBlockIds = new ArrayList<>();
  private int mInMemoryPercentage;
  private long mLastModificationTimeMs;
  private long mTtl;
  private TtlAction mTtlAction;
  private String mOwner = "";
  private String mGroup = "";
  private int mMode;
  private String mPersistenceState = "";
  private boolean mMountPoint;
  private ArrayList<FileBlockInfo> mFileBlockInfos = new ArrayList<>();
  private long mMountId;
  private int mInAlluxioPercentage;
  private String mUfsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
  // A list of String ACL entries, to support json serialization.
  private ArrayList<String> mAclEntries = new ArrayList<>();

  /**
   * Creates a new instance of {@link FileInfo}.
   */
  public FileInfo() {}

  /**
<<<<<<< HEAD
   * Creates a new instance of {@link FileInfo} from thrift representation.
   *
   * @param fileInfo the thrift representation of a file information
   */
  protected FileInfo(alluxio.thrift.FileInfo fileInfo) {
    mFileId = fileInfo.getFileId();
    mName = fileInfo.getName();
    mPath = fileInfo.getPath();
    mUfsPath = fileInfo.getUfsPath();
    mLength = fileInfo.getLength();
    mBlockSizeBytes = fileInfo.getBlockSizeBytes();
    mCreationTimeMs = fileInfo.getCreationTimeMs();
    mCompleted = fileInfo.isCompleted();
    mFolder = fileInfo.isFolder();
    mPinned = fileInfo.isPinned();
    mCacheable = fileInfo.isCacheable();
    mPersisted = fileInfo.isPersisted();
    mBlockIds = new ArrayList<>(fileInfo.getBlockIds());
    mInMemoryPercentage = fileInfo.getInMemoryPercentage();
    mLastModificationTimeMs = fileInfo.getLastModificationTimeMs();
    mTtl = fileInfo.getTtl();
    mTtlAction = ThriftUtils.fromThrift(fileInfo.getTtlAction());
    mOwner = fileInfo.getOwner();
    mGroup = fileInfo.getGroup();
    mMode = fileInfo.getMode();
    mPersistenceState = fileInfo.getPersistenceState();
    mMountPoint = fileInfo.isMountPoint();
    mFileBlockInfos = new ArrayList<>();
    if (fileInfo.getFileBlockInfos() != null) {
      for (alluxio.thrift.FileBlockInfo fileBlockInfo : fileInfo.getFileBlockInfos()) {
        mFileBlockInfos.add(new FileBlockInfo(fileBlockInfo));
      }
    }
    mMountId = fileInfo.getMountId();
    mInAlluxioPercentage = fileInfo.getInAlluxioPercentage();
    if (fileInfo.isSetUfsFingerprint()) {
      mUfsFingerprint = fileInfo.getUfsFingerprint();
    }
    if (fileInfo.isSetAcl()) {
      mAclEntries = new ArrayList<>();
      mAclEntries.addAll(AccessControlList.fromThrift(fileInfo.getAcl()).toStringEntries());
    }
  }

  /**
||||||| merged common ancestors
   * Creates a new instance of {@link FileInfo} from thrift representation.
   *
   * @param fileInfo the thrift representation of a file information
   */
  protected FileInfo(alluxio.thrift.FileInfo fileInfo) {
    mFileId = fileInfo.getFileId();
    mName = fileInfo.getName();
    mPath = fileInfo.getPath();
    mUfsPath = fileInfo.getUfsPath();
    mLength = fileInfo.getLength();
    mBlockSizeBytes = fileInfo.getBlockSizeBytes();
    mCreationTimeMs = fileInfo.getCreationTimeMs();
    mCompleted = fileInfo.isCompleted();
    mFolder = fileInfo.isFolder();
    mPinned = fileInfo.isPinned();
    mCacheable = fileInfo.isCacheable();
    mPersisted = fileInfo.isPersisted();
    mBlockIds = new ArrayList<>(fileInfo.getBlockIds());
    mInMemoryPercentage = fileInfo.getInMemoryPercentage();
    mLastModificationTimeMs = fileInfo.getLastModificationTimeMs();
    mTtl = fileInfo.getTtl();
    mTtlAction = ThriftUtils.fromThrift(fileInfo.getTtlAction());
    mOwner = fileInfo.getOwner();
    mGroup = fileInfo.getGroup();
    mMode = fileInfo.getMode();
    mPersistenceState = fileInfo.getPersistenceState();
    mMountPoint = fileInfo.isMountPoint();
    mFileBlockInfos = new ArrayList<>();
    if (fileInfo.getFileBlockInfos() != null) {
      for (alluxio.thrift.FileBlockInfo fileBlockInfo : fileInfo.getFileBlockInfos()) {
        mFileBlockInfos.add(new FileBlockInfo(fileBlockInfo));
      }
    }
    mMountId = fileInfo.getMountId();
    mInAlluxioPercentage = fileInfo.getInAlluxioPercentage();
    if (fileInfo.isSetUfsFingerprint()) {
      mUfsFingerprint = fileInfo.getUfsFingerprint();
    }
  }

  /**
=======
>>>>>>> master
   * @return the file id
   */
  public long getFileId() {
    return mFileId;
  }

  /**
   * @return the file name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the file path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * @return the file UFS path
   */
  public String getUfsPath() {
    return mUfsPath;
  }

  /**
   * @return the file length
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the file block size (in bytes)
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the file creation time (in milliseconds)
   */
  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  /**
   * @return whether the file is completed
   */
  public boolean isCompleted() {
    return mCompleted;
  }

  /**
   * @return whether the file is a folder
   */
  public boolean isFolder() {
    return mFolder;
  }

  /**
   * @return whether the file is pinned
   */
  public boolean isPinned() {
    return mPinned;
  }

  /**
   * @return whether the file is cacheable
   */
  public boolean isCacheable() {
    return mCacheable;
  }

  /**
   * @return whether the file is persisted
   */
  public boolean isPersisted() {
    return mPersisted;
  }

  /**
   * @return the file block ids
   */
  public List<Long> getBlockIds() {
    return mBlockIds;
  }

  /**
   * @return the file in memory percentage
   */
  public int getInMemoryPercentage() {
    return mInMemoryPercentage;
  }

  /**
   * @return the file in alluxio percentage
   */
  public int getInAlluxioPercentage() {
    return mInAlluxioPercentage;
  }

  /**
   * @return the file last modification time (in milliseconds)
   */
  public long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  /**
   * @return the file time-to-live (in seconds)
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return the {@link TtlAction}
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  /**
   * @return the file owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the file owner group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the file mode bits
   */
  public int getMode() {
    return mMode;
  }

  /**
   * @return the file persistence state
   */
  public String getPersistenceState() {
    return mPersistenceState;
  }

  /**
   * @return whether the file is a mount point
   */
  public boolean isMountPoint() {
    return mMountPoint;
  }

  /**
   * @return the list of file block descriptors
   */
  public List<FileBlockInfo> getFileBlockInfos() {
    return mFileBlockInfos;
  }

  /**
   * @return the id of the mount
   */
  public long getMountId() {
    return mMountId;
  }

  /**
   * @return the ufs fingerprint for this file
   */
  public String getUfsFingerprint() {
    return mUfsFingerprint;
  }

  /**
   * @return the ACL entries for this file
   */
  public List<String> getAclEntries() {
    return mAclEntries;
  }

  /**
   * @param fileId the file id to use
   * @return the file information
   */
  public FileInfo setFileId(long fileId) {
    mFileId = fileId;
    return this;
  }

  /**
   * @param name the file name to use
   * @return the file information
   */
  public FileInfo setName(String name) {
    Preconditions.checkNotNull(name, "name");
    mName = name;
    return this;
  }

  /**
   * @param path the file path to use
   * @return the file information
   */
  public FileInfo setPath(String path) {
    Preconditions.checkNotNull(path, "path");
    mPath = path;
    return this;
  }

  /**
   * @param ufsPath the file UFS path to use
   * @return the file information
   */
  public FileInfo setUfsPath(String ufsPath) {
    Preconditions.checkNotNull(ufsPath, "ufsPath");
    mUfsPath = ufsPath;
    return this;
  }

  /**
   * @param length the file length to use
   * @return the file information
   */
  public FileInfo setLength(long length) {
    mLength = length;
    return this;
  }

  /**
   * @param blockSizeBytes the file block size (in bytes) to use
   * @return the file information
   */
  public FileInfo setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * @param creationTimeMs the file creation time (in milliseconds) to use
   * @return the file information
   */
  public FileInfo setCreationTimeMs(long creationTimeMs) {
    mCreationTimeMs = creationTimeMs;
    return this;
  }

  /**
   * @param completed the completed flag value to use
   * @return the file information
   */
  public FileInfo setCompleted(boolean completed) {
    mCompleted = completed;
    return this;
  }

  /**
   * @param folder the folder flag value to use
   * @return the file information
   */
  public FileInfo setFolder(boolean folder) {
    mFolder = folder;
    return this;
  }

  /**
   * @param pinned the pinned flag value to use
   * @return the file information
   */
  public FileInfo setPinned(boolean pinned) {
    mPinned = pinned;
    return this;
  }

  /**
   * @param cacheable the cacheable flag value to use
   * @return the file information
   */
  public FileInfo setCacheable(boolean cacheable) {
    mCacheable = cacheable;
    return this;
  }

  /**
   * @param persisted the persisted flag value to use
   * @return the file information
   */
  public FileInfo setPersisted(boolean persisted) {
    mPersisted = persisted;
    return this;
  }

  /**
   * @param blockIds the file block ids to use
   * @return the file information
   */
  public FileInfo setBlockIds(List<Long> blockIds) {
    Preconditions.checkNotNull(blockIds, "blockIds");
    mBlockIds = new ArrayList<>(blockIds);
    return this;
  }

  /**
   * @param inMemoryPercentage the file in memory percentage to use
   * @return the file information
   */
  public FileInfo setInMemoryPercentage(int inMemoryPercentage) {
    mInMemoryPercentage = inMemoryPercentage;
    return this;
  }

  /**
   * @param inAlluxioPercentage the file in alluxio percentage to use
   * @return the file information
   */
  public FileInfo setInAlluxioPercentage(int inAlluxioPercentage) {
    mInAlluxioPercentage = inAlluxioPercentage;
    return this;
  }

  /**
   * @param lastModificationTimeMs the last modification time (in milliseconds) to use
   * @return the file information
   */
  public FileInfo setLastModificationTimeMs(long lastModificationTimeMs) {
    mLastModificationTimeMs = lastModificationTimeMs;
    return this;
  }

  /**
   * @param ttl the file time-to-live (in seconds) to use
   * @return the file information
   */
  public FileInfo setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public FileInfo setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return this;
  }

  /**
   * @param owner the file owner
   * @return the file information
   */
  public FileInfo setOwner(String owner) {
    Preconditions.checkNotNull(owner, "owner");
    mOwner = owner;
    return this;
  }

  /**
   * @param group the file group
   * @return the file information
   */
  public FileInfo setGroup(String group) {
    Preconditions.checkNotNull(group, "group");
    mGroup = group;
    return this;
  }

  /**
   * @param mode the file mode bits
   * @return the file information
   */
  public FileInfo setMode(int mode) {
    mMode = mode;
    return this;
  }

  /**
   * @param persistenceState the file persistence state to use
   * @return the file information
   */
  public FileInfo setPersistenceState(String persistenceState) {
    Preconditions.checkNotNull(persistenceState, "persistenceState");
    mPersistenceState = persistenceState;
    return this;
  }

  /**
   * @param mountPoint the mount point flag value to use
   * @return the file information
   */
  public FileInfo setMountPoint(boolean mountPoint) {
    mMountPoint = mountPoint;
    return this;
  }

  /**
   * @param fileBlockInfos the file block descriptors to use
   * @return the file information
   */
  public FileInfo setFileBlockInfos(List<FileBlockInfo> fileBlockInfos) {
    mFileBlockInfos = new ArrayList<>(fileBlockInfos);
    return this;
  }

  /**
   * @param mountId the id of mount
   * @return the file information
   */
  public FileInfo setMountId(long mountId) {
    mMountId = mountId;
    return this;
  }

  /**
   * @param ufsFingerprint the ufs fingerprint to use
   * @return the file information
   */
  public FileInfo setUfsFingerprint(String ufsFingerprint) {
    mUfsFingerprint = ufsFingerprint;
    return this;
  }

  /**
   * @param acl the ACL entries to use
   * @return the file information
   */
  public FileInfo setAclEntries(List<String> acl) {
    mAclEntries = new ArrayList<>();
    mAclEntries.addAll(acl);
    return this;
  }

  /**
   * @return thrift representation of the file information
   */
  public alluxio.thrift.FileInfo toThrift() {
    List<alluxio.thrift.FileBlockInfo> fileBlockInfos = new ArrayList<>();
    for (FileBlockInfo fileBlockInfo : mFileBlockInfos) {
      fileBlockInfos.add(fileBlockInfo.toThrift());
    }

    AccessControlList tAcl = AccessControlList.fromStringEntries(mOwner, mGroup, mAclEntries);

    alluxio.thrift.FileInfo info =
        new alluxio.thrift.FileInfo(mFileId, mName, mPath, mUfsPath, mLength, mBlockSizeBytes,
        mCreationTimeMs, mCompleted, mFolder, mPinned, mCacheable, mPersisted, mBlockIds,
        mInMemoryPercentage, mLastModificationTimeMs, mTtl, mOwner, mGroup, mMode,
<<<<<<< HEAD
        mPersistenceState, mMountPoint, fileBlockInfos, ThriftUtils.toThrift(mTtlAction), mMountId,
        mInAlluxioPercentage, mUfsFingerprint, tAcl.toThrift());
||||||| merged common ancestors
        mPersistenceState, mMountPoint, fileBlockInfos, ThriftUtils.toThrift(mTtlAction), mMountId,
        mInAlluxioPercentage, mUfsFingerprint);
=======
        mPersistenceState, mMountPoint, fileBlockInfos, TtlAction.toThrift(mTtlAction), mMountId,
        mInAlluxioPercentage, mUfsFingerprint);
>>>>>>> master
    return info;
  }

  /**
   * Creates a new instance of {@link FileInfo} from thrift representation.
   *
   * @param info the thrift representation of a file information
   * @return the instance
   */
  public static FileInfo fromThrift(alluxio.thrift.FileInfo info) {
    return new FileInfo()
        .setFileId(info.getFileId())
        .setName(info.getName())
        .setPath(info.getPath())
        .setUfsPath(info.getUfsPath())
        .setLength(info.getLength())
        .setBlockSizeBytes(info.getBlockSizeBytes())
        .setCreationTimeMs(info.getCreationTimeMs())
        .setCompleted(info.isCompleted())
        .setFolder(info.isFolder())
        .setPinned(info.isPinned())
        .setCacheable(info.isCacheable())
        .setPersisted(info.isPersisted())
        .setBlockIds(info.getBlockIds())
        .setInMemoryPercentage(info.getInMemoryPercentage())
        .setLastModificationTimeMs(info.getLastModificationTimeMs())
        .setTtl(info.getTtl())
        .setTtlAction(TtlAction.fromThrift(info.getTtlAction()))
        .setOwner(info.getOwner())
        .setGroup(info.getGroup())
        .setMode(info.getMode())
        .setPersistenceState(info.getPersistenceState())
        .setMountPoint(info.isMountPoint())
        .setFileBlockInfos(map(FileBlockInfo::fromThrift, info.getFileBlockInfos()))
        .setMountId(info.getMountId())
        .setInAlluxioPercentage(info.getInAlluxioPercentage())
        .setUfsFingerprint(info.isSetUfsFingerprint() ? info.getUfsFingerprint()
            : Constants.INVALID_UFS_FINGERPRINT);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileInfo)) {
      return false;
    }
    FileInfo that = (FileInfo) o;
    return mFileId == that.mFileId && mName.equals(that.mName) && mPath.equals(that.mPath)
        && mUfsPath.equals(that.mUfsPath) && mLength == that.mLength
        && mBlockSizeBytes == that.mBlockSizeBytes && mCreationTimeMs == that.mCreationTimeMs
        && mCompleted == that.mCompleted && mFolder == that.mFolder && mPinned == that.mPinned
        && mCacheable == that.mCacheable && mPersisted == that.mPersisted
        && mBlockIds.equals(that.mBlockIds) && mInMemoryPercentage == that.mInMemoryPercentage
        && mLastModificationTimeMs == that.mLastModificationTimeMs && mTtl == that.mTtl
        && mOwner.equals(that.mOwner) && mGroup.equals(that.mGroup) && mMode == that.mMode
        && mPersistenceState.equals(that.mPersistenceState) && mMountPoint == that.mMountPoint
        && mFileBlockInfos.equals(that.mFileBlockInfos) && mTtlAction == that.mTtlAction
        && mMountId == that.mMountId && mInAlluxioPercentage == that.mInAlluxioPercentage
        && mUfsFingerprint.equals(that.mUfsFingerprint)
        && mAclEntries.equals(that.mAclEntries);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFileId, mName, mPath, mUfsPath, mLength, mBlockSizeBytes,
        mCreationTimeMs, mCompleted, mFolder, mPinned, mCacheable, mPersisted, mBlockIds,
        mInMemoryPercentage, mLastModificationTimeMs, mTtl, mOwner, mGroup, mMode,
        mPersistenceState, mMountPoint, mFileBlockInfos, mTtlAction, mInAlluxioPercentage,
        mUfsFingerprint, mAclEntries);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("fileId", mFileId).add("name", mName).add("path", mPath)
        .add("ufsPath", mUfsPath).add("length", mLength).add("blockSizeBytes", mBlockSizeBytes)
        .add("creationTimeMs", mCreationTimeMs).add("completed", mCompleted).add("folder", mFolder)
        .add("pinned", mPinned).add("cacheable", mCacheable).add("persisted", mPersisted)
        .add("blockIds", mBlockIds).add("inMemoryPercentage", mInMemoryPercentage)
        .add("lastModificationTimesMs", mLastModificationTimeMs).add("ttl", mTtl)
        .add("ttlAction", mTtlAction).add("owner", mOwner).add("group", mGroup).add("mode", mMode)
        .add("persistenceState", mPersistenceState).add("mountPoint", mMountPoint)
        .add("fileBlockInfos", mFileBlockInfos)
        .add("mountId", mMountId).add("inAlluxioPercentage", mInAlluxioPercentage)
        .add("ufsFingerprint", mUfsFingerprint)
        .add("aclEntries", mAclEntries)
        .toString();
  }
}
