/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.wire;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * The file descriptor.
 */
@NotThreadSafe
// TODO(jiri): Consolidate with URIStatus
public final class FileInfo implements WireType<tachyon.thrift.FileInfo> {
  @JsonProperty("fileId")
  private long mFileId;
  @JsonProperty("name")
  private String mName = "";
  @JsonProperty("path")
  private String mPath = "";
  @JsonProperty("ufsPath")
  private String mUfsPath = "";
  @JsonProperty("length")
  private long mLength;
  @JsonProperty("blockSizeBytes")
  private long mBlockSizeBytes;
  @JsonProperty("creationTimeMs")
  private long mCreationTimeMs;
  @JsonProperty("completed")
  private boolean mCompleted;
  @JsonProperty("folder")
  private boolean mFolder;
  @JsonProperty("pinned")
  private boolean mPinned;
  @JsonProperty("cacheable")
  private boolean mCacheable;
  @JsonProperty("persisted")
  private boolean mPersisted;
  @JsonProperty("blockIds")
  private List<Long> mBlockIds = Lists.newArrayList();
  @JsonProperty("inMemoryPercentage")
  private int mInMemoryPercentage;
  @JsonProperty("lastModificationTimeMs")
  private long mLastModificationTimeMs;
  @JsonProperty("ttl")
  private long mTtl;
  @JsonProperty("userName")
  private String mUserName = "";
  @JsonProperty("groupName")
  private String mGroupName = "";
  @JsonProperty("permission")
  private int mPermission;
  @JsonProperty("persistenceState")
  private String mPersistenceState = "";

  /**
   * Creates a new instance of {@link FileInfo}.
   */
  public FileInfo() {}

  /**
   * Creates a new instance of {@link FileInfo} from thrift representation.
   *
   * @param fileInfo the thrift representation of a file descriptor
   */
  public FileInfo(tachyon.thrift.FileInfo fileInfo) {
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
    mBlockIds = fileInfo.getBlockIds();
    mInMemoryPercentage = fileInfo.getInMemoryPercentage();
    mLastModificationTimeMs = fileInfo.getLastModificationTimeMs();
    mTtl = fileInfo.getTtl();
    mUserName = fileInfo.getUserName();
    mGroupName = fileInfo.getGroupName();
    mPermission = fileInfo.getPermission();
    mPersistenceState = fileInfo.getPersistenceState();
  }

  /**
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
   * @return the file owner user name
   */
  public String getUserName() {
    return mUserName;
  }

  /**
   * @return the file owner group name
   */
  public String getGroupName() {
    return mGroupName;
  }

  /**
   * @return the file permission bits
   */
  public int getPermission() {
    return mPermission;
  }

  /**
   * @return the file persistence state
   */
  public String getPersistenceState() {
    return mPersistenceState;
  }

  /**
   * @param fileId the file id to use
   * @return the file descriptor
   */
  public FileInfo setFileId(long fileId) {
    mFileId = fileId;
    return this;
  }

  /**
   * @param name the file name to use
   * @return the file descriptor
   */
  public FileInfo setName(String name) {
    Preconditions.checkNotNull(name);
    mName = name;
    return this;
  }

  /**
   * @param path the file path to use
   * @return the file descriptor
   */
  public FileInfo setPath(String path) {
    Preconditions.checkNotNull(path);
    mPath = path;
    return this;
  }

  /**
   * @param ufsPath the file UFS path to use
   * @return the file descriptor
   */
  public FileInfo setUfsPath(String ufsPath) {
    Preconditions.checkNotNull(ufsPath);
    mUfsPath = ufsPath;
    return this;
  }

  /**
   * @param length the file length to use
   * @return the file descriptor
   */
  public FileInfo setLength(long length) {
    mLength = length;
    return this;
  }

  /**
   * @param blockSizeBytes the file block size (in bytes) to use
   * @return the file descriptor
   */
  public FileInfo setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * @param creationTimeMs the file creation time (in milliseconds) to use
   * @return the file descriptor
   */
  public FileInfo setCreationTimeMs(long creationTimeMs) {
    mCreationTimeMs = creationTimeMs;
    return this;
  }

  /**
   * @param completed the completed flag value to use
   * @return the file descriptor
   */
  public FileInfo setCompleted(boolean completed) {
    mCompleted = completed;
    return this;
  }

  /**
   * @param folder the folder flag value to use
   * @return the file descriptor
   */
  public FileInfo setFolder(boolean folder) {
    mFolder = folder;
    return this;
  }

  /**
   * @param pinned the pinned flag value to use
   * @return the file descriptor
   */
  public FileInfo setPinned(boolean pinned) {
    mPinned = pinned;
    return this;
  }

  /**
   * @param cacheable the cacheable flag value to use
   * @return the file descriptor
   */
  public FileInfo setCacheable(boolean cacheable) {
    mCacheable = cacheable;
    return this;
  }

  /**
   * @param persisted the persisted flag value to use
   * @return the file descriptor
   */
  public FileInfo setPersisted(boolean persisted) {
    mPersisted = persisted;
    return this;
  }

  /**
   * @param blockIds the file block ids to use
   * @return the file descriptor
   */
  public FileInfo setBlockIds(List<Long> blockIds) {
    Preconditions.checkNotNull(blockIds);
    mBlockIds = blockIds;
    return this;
  }

  /**
   * @param inMemoryPercentage the file in memory percentage to use
   * @return the file descriptor
   */
  public FileInfo setInMemoryPercentage(int inMemoryPercentage) {
    mInMemoryPercentage = inMemoryPercentage;
    return this;
  }

  /**
   * @param lastModificationTimeMs the last modification time (in milliseconds) to use
   * @return the file descriptor
   */
  public FileInfo setLastModificationTimeMs(long lastModificationTimeMs) {
    mLastModificationTimeMs = lastModificationTimeMs;
    return this;
  }

  /**
   * @param ttl the file time-to-live (in seconds) to use
   * @return the file descriptor
   */
  public FileInfo setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @param userName the file owner user name to use
   * @return the file descriptor
   */
  public FileInfo setUserName(String userName) {
    Preconditions.checkNotNull(userName);
    mUserName = userName;
    return this;
  }

  /**
   * @param groupName the file owner group name to use
   * @return the file descriptor
   */
  public FileInfo setGroupName(String groupName) {
    Preconditions.checkNotNull(groupName);
    mGroupName = groupName;
    return this;
  }

  /**
   * @param permission the file permission bits to use
   * @return the file descriptor
   */
  public FileInfo setPermission(int permission) {
    mPermission = permission;
    return this;
  }

  /**
   * @param persistenceState the file persistance state to use
   * @return the file descriptor
   */
  public FileInfo setPersistenceState(String persistenceState) {
    Preconditions.checkNotNull(persistenceState);
    mPersistenceState = persistenceState;
    return this;
  }

  /**
   * @return thrift representation of the file descriptor
   */
  @Override
  public tachyon.thrift.FileInfo toThrift() {
    return new tachyon.thrift.FileInfo(mFileId, mName, mPath, mUfsPath, mLength, mBlockSizeBytes,
        mCreationTimeMs, mCompleted, mFolder, mPinned, mCacheable, mPersisted, mBlockIds,
        mInMemoryPercentage, mLastModificationTimeMs, mTtl, mUserName, mGroupName, mPermission,
        mPersistenceState);
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
        && mUserName.equals(that.mUserName) && mGroupName.equals(that.mGroupName)
        && mPermission == that.mPermission && mPersistenceState.equals(that.mPersistenceState);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFileId, mName, mPath, mUfsPath, mLength, mBlockSizeBytes,
        mCreationTimeMs, mCompleted, mFolder, mPinned, mCacheable, mPersisted, mBlockIds,
        mInMemoryPercentage, mLastModificationTimeMs, mTtl, mUserName, mGroupName, mPermission,
        mPersistenceState);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("fileId", mFileId).add("name", mName)
        .add("path", mPath).add("ufsPath", mUfsPath).add("length", mLength)
        .add("blockSizeBytes", mBlockSizeBytes).add("creationTimeMs", mCreationTimeMs)
        .add("completed", mCompleted).add("folder", mFolder).add("pinned", mPinned)
        .add("cacheable", mCacheable).add("persisted", mPersisted)
        .add("blockIds", mBlockIds).add("inMemoryPercentage", mInMemoryPercentage)
        .add("lastModificationTimesMs", mLastModificationTimeMs).add("ttl", mTtl)
        .add("userName", mUserName).add("groupName", mGroupName).add("permission", mPermission)
        .add("persistanceState", mPersistenceState).toString();
  }
}
