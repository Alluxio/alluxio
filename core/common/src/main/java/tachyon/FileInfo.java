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

package tachyon;

import java.util.List;

/**
 * The file descriptor.
 */
public class FileInfo {
  private long mFileId;
  private String mName;
  private String mPath;
  private String mUfsPath;
  private long mLength;
  private long mBlockSizeBytes;
  private long mCreationTimeMs;
  private boolean mCompleted;
  private boolean mFolder;
  private boolean mPinned;
  private boolean mCacheable;
  private boolean mPersisted;
  private List<Long> mBlockIds;
  private int mInMemoryPercentage;
  private long mLastModificationTimeMs;
  private long mTtl;
  private String mUserName;
  private String mGroupName;
  private int mPermission;
  private String mPersistenceState;

  /**
   * Creates a new instance of {@link FileInfo}.
   */
  public FileInfo() {}

  /**
   * Creates a new instance of {@link FileInfo} from thrift representation.
   *
   * @param fileInfo the thrift representation to use
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
   */
  public void setFileId(long fileId) {
    mFileId = fileId;
  }

  /**
   * @param name the file name to use
   */
  public void setName(String name) {
    mName = name;
  }

  /**
   * @param path the file path to use
   */
  public void setPath(String path) {
    mPath = path;
  }

  /**
   * @param ufsPath the file UFS path to use
   */
  public void setUfsPath(String ufsPath) {
    mUfsPath = ufsPath;
  }

  /**
   * @param length the file length to use
   */
  public void setLength(long length) {
    mLength = length;
  }

  /**
   * @param blockSizeBytes the file block size (in bytes) to use
   */
  public void setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
  }

  /**
   * @param creationTimeMs the file creation time (in milliseconds) to use
   */
  public void setCreationTimeMs(long creationTimeMs) {
    mCreationTimeMs = creationTimeMs;
  }

  /**
   * @param completed the completed flag value to use
   */
  public void setCompleted(boolean completed) {
    mCompleted = completed;
  }

  /**
   * @param folder the folder flag value to use
   */
  public void setFolder(boolean folder) {
    mFolder = folder;
  }

  /**
   * @param pinned the pinned flag value to use
   */
  public void setPinned(boolean pinned) {
    mPinned = pinned;
  }

  /**
   * @param cacheable the cacheable flag value to use
   */
  public void setCacheable(boolean cacheable) {
    mCacheable = cacheable;
  }

  /**
   * @param persisted the persisted flag value to use
   */
  public void setPersisted(boolean persisted) {
    mPersisted = persisted;
  }

  /**
   * @param blockIds the file block ids to use
   */
  public void setBlockIds(List<Long> blockIds) {
    mBlockIds = blockIds;
  }

  /**
   * @param inMemoryPercentage the file in memory percentage to use
   */
  public void setInMemoryPercentage(int inMemoryPercentage) {
    mInMemoryPercentage = inMemoryPercentage;
  }

  /**
   * @param lastModificationTimeMs the last modification time (in milliseconds) to use
   */
  public void setLastModificationTimeMs(long lastModificationTimeMs) {
    mLastModificationTimeMs = lastModificationTimeMs;
  }

  /**
   * @param ttl the file time-to-live (in seconds) to use
   */
  public void setTtl(long ttl) {
    mTtl = ttl;
  }

  /**
   * @param userName the file owner user name to use
   */
  public void setUserName(String userName) {
    mUserName = userName;
  }

  /**
   * @param groupName the file owner group name to use
   */
  public void setGroupName(String groupName) {
    mGroupName = groupName;
  }

  /**
   * @param permission the file permission bits to use
   */
  public void setPermission(int permission) {
    mPermission = permission;
  }

  /**
   * @param persistenceState the file persistance state to use
   */
  public void setPersistenceState(String persistenceState) {
    mPersistenceState = persistenceState;
  }

  /**
   * @return thrift representation of the file descriptor
   */
  public tachyon.thrift.FileInfo toThrift() {
    return new tachyon.thrift.FileInfo(mFileId, mName, mPath, mUfsPath, mLength, mBlockSizeBytes,
        mCreationTimeMs, mCompleted, mFolder, mPinned, mCacheable, mPersisted, mBlockIds,
        mInMemoryPercentage, mLastModificationTimeMs, mTtl, mUserName, mGroupName, mPermission,
        mPersistenceState);
  }
}
