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

package alluxio.web;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.FileInfo;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Contains information about a file to be displayed in the UI.
 */
@ThreadSafe
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
  @ThreadSafe
  static class LocalFileInfo {
    public static final long EMPTY_CREATION_TIME = 0;

    private final String mName;
    private final String mAbsolutePath;
    private final long mSize;
    private final long mCreationTimeMs;
    private final long mLastModificationTimeMs;
    private final boolean mIsDirectory;

    /**
     * Creates a new instance of {@link UIFileInfo.LocalFileInfo}.
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
      mName = Preconditions.checkNotNull(name, "name");
      mAbsolutePath = Preconditions.checkNotNull(absolutePath, "absolutePath");
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
  private final String mOwner;
  private final String mGroup;
  private final String mMode;
  private final String mPersistenceState;
  private final List<String> mFileLocations;

  private final Map<String, List<UIFileBlockInfo>> mBlocksOnTier = new HashMap<>();
  private final Map<String, Long> mSizeOnTier = new HashMap<>();

  /**
   * Creates a new instance of {@link UIFileInfo}.
   *
   * @param status underlying {@link URIStatus}
   */
  public UIFileInfo(URIStatus status) {
    mId = status.getFileId();
    mName = status.getName();
    mAbsolutePath = status.getPath();
    mBlockSizeBytes = status.getBlockSizeBytes();
    mSize = status.getLength();
    mCreationTimeMs = status.getCreationTimeMs();
    mLastModificationTimeMs = status.getLastModificationTimeMs();
    mInMemory = (100 == status.getInMemoryPercentage());
    mInMemoryPercent = status.getInMemoryPercentage();
    mIsDirectory = status.isFolder();
    mPinned = status.isPinned();
    mOwner = status.getOwner();
    mGroup = status.getGroup();
    mMode = FormatUtils.formatMode((short) status.getMode(), status.isFolder());
    mPersistenceState = status.getPersistenceState();
    mFileLocations = new ArrayList<>();
  }

  /**
   * Creates a new instance of {@link UIFileInfo}.
   *
   * @param info underlying {@link FileInfo}
   */
  public UIFileInfo(FileInfo info) {
    this(new URIStatus(info));
  }

  /**
   * Creates a new instance of {@link UIFileInfo}.
   *
   * @param fileInfo underlying {@link UIFileInfo.LocalFileInfo}
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
    mOwner = "";
    mGroup = "";
    mMode = FormatUtils.formatMode(Mode.createNoAccess().toShort(), true);
    mPersistenceState = PersistenceState.NOT_PERSISTED.name();
    mFileLocations = new ArrayList<>();
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
      blocksOnTier = new ArrayList<>();
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
    return CommonUtils.convertMsToDate(mCreationTimeMs);
  }

  /**
   * @return the modification time (in milliseconds)
   */
  public String getModificationTime() {
    return CommonUtils.convertMsToDate(mLastModificationTimeMs);
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
    Long sizeOnTier = mSizeOnTier.containsKey(tierAlias) ? mSizeOnTier.get(tierAlias) : 0L;
    return (int) (100 * sizeOnTier / mSize);
  }

  /**
   * @return the file name
   */
  public String getName() {
    if (AlluxioURI.SEPARATOR.equals(mAbsolutePath)) {
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
  public void setFileLocations(List<String> fileLocations) {
    mFileLocations.clear();
    mFileLocations.addAll(fileLocations);
  }

  /**
   * @return the owner of the file
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the group of the file
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the mode of the file
   */
  public String getMode() {
    return mMode;
  }
}
