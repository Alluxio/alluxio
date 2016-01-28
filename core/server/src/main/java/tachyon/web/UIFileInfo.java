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

package tachyon.web;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import tachyon.TachyonURI;
import tachyon.master.file.meta.PersistenceState;
import tachyon.security.authorization.FileSystemPermission;
import tachyon.thrift.FileInfo;
import tachyon.thrift.WorkerNetAddress;
import tachyon.util.FormatUtils;

/**
 * Contains information about a file to be displayed in the UI.
 */
public final class UIFileInfo {
  /**
   * Provides ordering of {@link UIFileInfo} based off a string comparison of the
   * absolute paths.
   */
  public static final Ordering<UIFileInfo> PATH_STRING_COMPARE =
      Ordering.natural().onResultOf(new Function<UIFileInfo, Comparable<String>>() {
        @Override
        public Comparable<String> apply(UIFileInfo input) {
          return input.mAbsolutePath;
        }
      });

  // Simple class for describing a file on the local filesystem.
  static class LocalFileInfo {
    public static final long EMPTY_CREATION_TIME = 0;

    private final String mName;
    private final String mAbsolutePath;
    private final long mSize;
    private final long mCreationTimeMs;
    private final long mLastModificationTimeMs;
    private final boolean mIsDirectory;

    /**
     * Creates a new instance of {@link LocalFileInfo}.
     *
     * @param name name
     * @param absolutePath absolute path
     * @param size size
     * @param creationTimeMs creation time in milliseconds
     * @param lastModificationTimeMs last modification time in milliseconds
     * @param isDirectory whether the object represents a directory
     */
    public LocalFileInfo(String name, String absolutePath, long size, long creationTimeMs,
        long lastModificationTimeMs, boolean isDirectory) {
      mName = Preconditions.checkNotNull(name);
      mAbsolutePath = Preconditions.checkNotNull(absolutePath);
      mSize = size;
      mCreationTimeMs = creationTimeMs;
      mLastModificationTimeMs = lastModificationTimeMs;
      mIsDirectory = isDirectory;
    }
  }

  private final long mId;
  private final String mName;
  private final String mAbsolutePath;
  private final long mBlockSizeBytes;
  private final long mSize;
  private final long mCreationTimeMs;
  private final long mLastModificationTimeMs;
  private final boolean mInMemory;
  private final int mInMemoryPercent;
  private final boolean mIsDirectory;
  private final boolean mPinned;
  private final String mUserName;
  private final String mGroupName;
  private final String mPermission;
  private final String mPersistenceState;
  private List<String> mFileLocations;

  private final Map<String, List<UIFileBlockInfo>> mBlocksOnTier =
      new HashMap<String, List<UIFileBlockInfo>>();
  private final Map<String, Long> mSizeOnTier = new HashMap<String, Long>();

  /**
   * Creates a new instance of {@link UIFileInfo}.
   *
   * @param fileInfo underlying {@link FileInfo}
   */
  public UIFileInfo(FileInfo fileInfo) {
    mId = fileInfo.getFileId();
    mName = fileInfo.getName();
    mAbsolutePath = fileInfo.getPath();
    mBlockSizeBytes = fileInfo.getBlockSizeBytes();
    mSize = fileInfo.getLength();
    mCreationTimeMs = fileInfo.getCreationTimeMs();
    mLastModificationTimeMs = fileInfo.getLastModificationTimeMs();
    mInMemory = (100 == fileInfo.getInMemoryPercentage());
    mInMemoryPercent = fileInfo.getInMemoryPercentage();
    mIsDirectory = fileInfo.isFolder();
    mPinned = fileInfo.isPinned();
    mUserName = fileInfo.getUserName();
    mGroupName = fileInfo.getGroupName();
    mPermission =
        FormatUtils.formatPermission((short) fileInfo.getPermission(), fileInfo.isFolder());
    mPersistenceState = fileInfo.getPersistenceState();
    mFileLocations = new ArrayList<String>();
  }

  /**
   * Creates a new instance of {@link UIFileInfo}.
   *
   * @param fileInfo underlying {@link LocalFileInfo}
   */
  public UIFileInfo(LocalFileInfo fileInfo) {
    mId = -1;
    mName = fileInfo.mName;
    mAbsolutePath = fileInfo.mAbsolutePath;
    mBlockSizeBytes = 0;
    mSize = fileInfo.mSize;
    mCreationTimeMs = fileInfo.mCreationTimeMs;
    mLastModificationTimeMs = fileInfo.mLastModificationTimeMs;
    mInMemory = false;
    mInMemoryPercent = 0;
    mIsDirectory = fileInfo.mIsDirectory;
    mPinned = false;
    mUserName = "";
    mGroupName = "";
    mPermission =
        FormatUtils.formatPermission((short) FileSystemPermission.getNoneFsPermission()
            .toShort(), true);
    mPersistenceState = PersistenceState.NOT_PERSISTED.name();
    mFileLocations = new ArrayList<String>();
  }

  /**
   * Adds a block to the file information.
   *
   * @param tierAlias the tier alias
   * @param blockId the block id
   * @param blockSize the block size
   * @param blockLastAccessTimeMs the last access time (in milliseconds)
   */
  public void addBlock(String tierAlias, long blockId, long blockSize, long blockLastAccessTimeMs) {
    UIFileBlockInfo block =
        new UIFileBlockInfo(blockId, blockSize, blockLastAccessTimeMs, tierAlias);
    List<UIFileBlockInfo> blocksOnTier = mBlocksOnTier.get(tierAlias);
    if (blocksOnTier == null) {
      blocksOnTier = new ArrayList<UIFileBlockInfo>();
      mBlocksOnTier.put(tierAlias, blocksOnTier);
    }
    blocksOnTier.add(block);

    Long sizeOnTier = mSizeOnTier.get(tierAlias);
    mSizeOnTier.put(tierAlias, (sizeOnTier == null ? 0L : sizeOnTier) + blockSize);
  }

  /**
   * @return the absolute path
   */
  public String getAbsolutePath() {
    return mAbsolutePath;
  }

  /**
   * @return the block size (in bytes)
   */
  public String getBlockSizeBytes() {
    if (mIsDirectory) {
      return "";
    } else {
      return FormatUtils.getSizeFromBytes(mBlockSizeBytes);
    }
  }

  /**
   * @return a mapping from tiers to file blocks
   */
  public Map<String, List<UIFileBlockInfo>> getBlocksOnTier() {
    return mBlocksOnTier;
  }

  /**
   * @return the creation time (in milliseconds)
   */
  public String getCreationTime() {
    if (mCreationTimeMs == LocalFileInfo.EMPTY_CREATION_TIME) {
      return "";
    }
    return WebUtils.convertMsToDate(mCreationTimeMs);
  }

  /**
   * @return the modification time (in milliseconds)
   */
  public String getModificationTime() {
    return WebUtils.convertMsToDate(mLastModificationTimeMs);
  }

  /**
   * @return the file locations
   */
  public List<String> getFileLocations() {
    return mFileLocations;
  }

  /**
   * @return the file id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return whether the file is present in memory
   */
  public boolean getInMemory() {
    return mInMemory;
  }

  /**
   * @return the percentage of the file present in memory
   */
  public int getInMemoryPercentage() {
    return mInMemoryPercent;
  }

  /**
   * @return whether the object represents a directory
   */
  public boolean getIsDirectory() {
    return mIsDirectory;
  }

  /**
   * @return whether the file is pinned
   */
  public boolean isPinned() {
    return mPinned;
  }

  /**
   * @return the {@link PersistenceState} of the file
   */
  public String getPersistenceState() {
    return mPersistenceState;
  }

  /**
   * @param tierAlias a tier alias
   * @return the percentage of the file stored in the given tier
   */
  public int getOnTierPercentage(String tierAlias) {
    return (int) (100 * mSizeOnTier.get(tierAlias) / mSize);
  }

  /**
   * @return the file name
   */
  public String getName() {
    if (TachyonURI.SEPARATOR.equals(mAbsolutePath)) {
      return "root";
    } else {
      return mName;
    }
  }

  /**
   * @return the file size
   */
  public String getSize() {
    if (mIsDirectory) {
      return "";
    } else {
      return FormatUtils.getSizeFromBytes(mSize);
    }
  }

  /**
   * @param fileLocations the file locations to use
   */
  public void setFileLocations(List<WorkerNetAddress> fileLocations) {
    for (WorkerNetAddress addr : fileLocations) {
      mFileLocations.add(addr.getHost() + ":" + addr.getRpcPort());
    }
  }

  /**
   * @return the user name of the file
   */
  public String getUserName() {
    return mUserName;
  }

  /**
   * @return the group name of the file
   */
  public String getGroupName() {
    return mGroupName;
  }

  /**
   * @return the permission of the file
   */
  public String getPermission() {
    return mPermission;
  }
}
