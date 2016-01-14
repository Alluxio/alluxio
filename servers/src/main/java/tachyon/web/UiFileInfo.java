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
import tachyon.thrift.NetAddress;
import tachyon.util.FormatUtils;

public final class UiFileInfo {
  /**
   * Provides ordering of {@link tachyon.web.UiFileInfo} based off a string comparison of the
   * absolute paths.
   */
  public static final Ordering<UiFileInfo> PATH_STRING_COMPARE =
      Ordering.natural().onResultOf(new Function<UiFileInfo, Comparable<String>>() {
        @Override
        public Comparable<String> apply(UiFileInfo input) {
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
  private final boolean mIsPinned;
  private final String mUserName;
  private final String mGroupName;
  private final String mPermission;
  private final String mPersistenceState;
  private List<String> mFileLocations;

  private final Map<String, List<UiBlockInfo>> mBlocksOnTier =
      new HashMap<String, List<UiBlockInfo>>();
  private final Map<String, Long> mSizeOnTier = new HashMap<String, Long>();

  public UiFileInfo(FileInfo fileInfo) {
    mId = fileInfo.getFileId();
    mName = fileInfo.getName();
    mAbsolutePath = fileInfo.getPath();
    mBlockSizeBytes = fileInfo.getBlockSizeBytes();
    mSize = fileInfo.getLength();
    mCreationTimeMs = fileInfo.getCreationTimeMs();
    mLastModificationTimeMs = fileInfo.getLastModificationTimeMs();
    mInMemory = (100 == fileInfo.inMemoryPercentage);
    mInMemoryPercent = fileInfo.getInMemoryPercentage();
    mIsDirectory = fileInfo.isFolder;
    mIsPinned = fileInfo.isPinned;
    mUserName = fileInfo.getUserName();
    mGroupName = fileInfo.getGroupName();
    mPermission =
        FormatUtils.formatFilePermission((short) fileInfo.getPermission(), fileInfo.isFolder);
    mPersistenceState = fileInfo.persistenceState;
    mFileLocations = new ArrayList<String>();
  }

  public UiFileInfo(LocalFileInfo fileInfo) {
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
    mIsPinned = false;
    mUserName = "";
    mGroupName = "";
    mPermission =
        FormatUtils.formatFilePermission((short) FileSystemPermission.getNoneFsPermission()
            .toShort(), true);
    mPersistenceState = PersistenceState.NOT_PERSISTED.name();
    mFileLocations = new ArrayList<String>();
  }

  public void addBlock(String tierAlias, long blockId, long blockSize, long blockLastAccessTimeMs) {
    UiBlockInfo block = new UiBlockInfo(blockId, blockSize, blockLastAccessTimeMs, tierAlias);
    List<UiBlockInfo> blocksOnTier = mBlocksOnTier.get(tierAlias);
    if (blocksOnTier == null) {
      blocksOnTier = new ArrayList<UiBlockInfo>();
      mBlocksOnTier.put(tierAlias, blocksOnTier);
    }
    blocksOnTier.add(block);

    Long sizeOnTier = mSizeOnTier.get(tierAlias);
    mSizeOnTier.put(tierAlias, (sizeOnTier == null ? 0L : sizeOnTier) + blockSize);
  }

  public String getAbsolutePath() {
    return mAbsolutePath;
  }

  public String getBlockSizeBytes() {
    if (mIsDirectory) {
      return "";
    } else {
      return FormatUtils.getSizeFromBytes(mBlockSizeBytes);
    }
  }

  public Map<String, List<UiBlockInfo>> getBlocksOnTier() {
    return mBlocksOnTier;
  }

  public String getCreationTime() {
    if (mCreationTimeMs == LocalFileInfo.EMPTY_CREATION_TIME) {
      return "";
    }
    return Utils.convertMsToDate(mCreationTimeMs);
  }

  public String getModificationTime() {
    return Utils.convertMsToDate(mLastModificationTimeMs);
  }

  public List<String> getFileLocations() {
    return mFileLocations;
  }

  public long getId() {
    return mId;
  }

  public boolean getInMemory() {
    return mInMemory;
  }

  public int getInMemoryPercentage() {
    return mInMemoryPercent;
  }

  public boolean getIsDirectory() {
    return mIsDirectory;
  }

  public boolean getNeedPin() {
    return mIsPinned;
  }

  public String getPersistenceState() {
    return mPersistenceState;
  }

  public int getOnTierPercentage(String tierAlias) {
    return (int) (100 * mSizeOnTier.get(tierAlias) / mSize);
  }

  public String getName() {
    if (TachyonURI.SEPARATOR.equals(mAbsolutePath)) {
      return "root";
    } else {
      return mName;
    }
  }

  public String getSize() {
    if (mIsDirectory) {
      return "";
    } else {
      return FormatUtils.getSizeFromBytes(mSize);
    }
  }

  public void setFileLocations(List<NetAddress> fileLocations) {
    for (NetAddress addr : fileLocations) {
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
