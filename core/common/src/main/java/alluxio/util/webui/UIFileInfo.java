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

package alluxio.util.webui;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
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
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Contains information about a file to be displayed in the UI.
 */
@ThreadSafe
public final class UIFileInfo {
  /**
   * Provides ordering of {@link alluxio.util.webui.UIFileInfo} based off a string comparison of the
   * absolute paths.
   */
  public static final Ordering<UIFileInfo> PATH_STRING_COMPARE = Ordering.natural()
      .onResultOf((Function<UIFileInfo, Comparable<String>>) input -> input.mAbsolutePath);

  /**
   * The type Local file info.
   */
  @ThreadSafe
  public static class LocalFileInfo {
    /**
     * The constant EMPTY_CREATION_TIME.
     */
    public static final long EMPTY_CREATION_TIME = 0;

    private final String mName;
    private final String mAbsolutePath;
    private final long mSize;
    private final long mCreationTimeMs;
    private final long mLastModificationTimeMs;
    private final boolean mIsDirectory;

    /**
     * Creates a new instance of {@link alluxio.util.webui.UIFileInfo.LocalFileInfo}.
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
  private final boolean mInAlluxio;
  private final int mInAlluxioPercentage;
  private final boolean mIsDirectory;
  private final boolean mPinned;
  private final String mOwner;
  private final String mGroup;
  private final String mMode;
  private final String mPersistenceState;
  private final List<String> mFileLocations;
  private final AlluxioConfiguration mAlluxioConfiguration;
  private final List<String> mOrderedTierAliases;

  private final Map<String, List<UIFileBlockInfo>> mBlocksOnTier = new HashMap<>();
  private final Map<String, Long> mSizeOnTier = new HashMap<>();
  private final List<Long> mBlockIds = new ArrayList<>();

  /**
   * Creates a new instance of {@link alluxio.util.webui.UIFileInfo}.
   *
   * @param status underlying {@link URIStatus}
   * @param alluxioConfiguration the alluxio configuration
   * @param orderedTierAliases the ordered tier aliases
   */
  public UIFileInfo(URIStatus status, AlluxioConfiguration alluxioConfiguration,
      List<String> orderedTierAliases) {
    // detect the extended acls
    boolean hasExtended = status.getAcl().hasExtended() || !status.getDefaultAcl().isEmpty();

    mId = status.getFileId();
    mName = status.getName();
    mAbsolutePath = status.getPath();
    mBlockSizeBytes = status.getBlockSizeBytes();
    mSize = status.getLength();
    mCreationTimeMs = status.getCreationTimeMs();
    mLastModificationTimeMs = status.getLastModificationTimeMs();
    mInAlluxio = (100 == status.getInAlluxioPercentage());
    mInAlluxioPercentage = status.getInAlluxioPercentage();
    mIsDirectory = status.isFolder();
    mPinned = status.isPinned();
    mOwner = status.getOwner();
    mGroup = status.getGroup();
    mMode = FormatUtils.formatMode((short) status.getMode(), status.isFolder(), hasExtended);
    mPersistenceState = status.getPersistenceState();
    mFileLocations = new ArrayList<>();
    mAlluxioConfiguration = alluxioConfiguration;
    mOrderedTierAliases = orderedTierAliases;
  }

  /**
   * Creates a new instance of {@link alluxio.util.webui.UIFileInfo}.
   *
   * @param info underlying {@link FileInfo}
   * @param alluxioConfiguration the alluxio configuration
   * @param orderedTierAliases the ordered tier aliases
   */
  public UIFileInfo(FileInfo info, AlluxioConfiguration alluxioConfiguration,
      List<String> orderedTierAliases) {
    this(new URIStatus(info), alluxioConfiguration, orderedTierAliases);
  }

  /**
   * Creates a new instance of {@link alluxio.util.webui.UIFileInfo}.
   *
   * @param fileInfo underlying {@link alluxio.util.webui.UIFileInfo.LocalFileInfo}
   * @param alluxioConfiguration the alluxio configuration
   * @param orderedTierAliases the ordered tier aliases
   */
  public UIFileInfo(UIFileInfo.LocalFileInfo fileInfo, AlluxioConfiguration alluxioConfiguration,
      List<String> orderedTierAliases) {
    mId = -1;
    mName = fileInfo.mName;
    mAbsolutePath = fileInfo.mAbsolutePath;
    mBlockSizeBytes = 0;
    mSize = fileInfo.mSize;
    mCreationTimeMs = fileInfo.mCreationTimeMs;
    mLastModificationTimeMs = fileInfo.mLastModificationTimeMs;
    mInAlluxio = false;
    mInAlluxioPercentage = 0;
    mIsDirectory = fileInfo.mIsDirectory;
    mPinned = false;
    mOwner = "";
    mGroup = "";
    mMode = FormatUtils.formatMode(Mode.createNoAccess().toShort(), true, false);
    mPersistenceState = PersistenceState.NOT_PERSISTED.name();
    mFileLocations = new ArrayList<>();
    mAlluxioConfiguration = alluxioConfiguration;
    mOrderedTierAliases = orderedTierAliases;
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
        new UIFileBlockInfo(blockId, blockSize, blockLastAccessTimeMs, tierAlias,
            mAlluxioConfiguration);
    List<UIFileBlockInfo> blocksOnTier = mBlocksOnTier.computeIfAbsent(tierAlias,
        k -> new ArrayList<>());
    blocksOnTier.add(block);

    Long sizeOnTier = mSizeOnTier.get(tierAlias);
    mSizeOnTier.put(tierAlias, (sizeOnTier == null ? 0L : sizeOnTier) + blockSize);
    mBlockIds.add(blockId);
  }

  /**
   * Gets absolute path.
   *
   * @return the absolute path
   */
  public String getAbsolutePath() {
    return mAbsolutePath;
  }

  /**
   * Gets block size bytes.
   *
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
   * Gets blocks on tier.
   *
   * @return a mapping from tiers to file blocks
   */
  public Map<String, List<UIFileBlockInfo>> getBlocksOnTier() {
    return mBlocksOnTier;
  }

  /**
   * Gets creation time.
   *
   * @return the creation time (in milliseconds)
   */
  public String getCreationTime() {
    if (mCreationTimeMs == UIFileInfo.LocalFileInfo.EMPTY_CREATION_TIME) {
      return "";
    }
    return CommonUtils.convertMsToDate(mCreationTimeMs,
        mAlluxioConfiguration.get(PropertyKey.USER_DATE_FORMAT_PATTERN));
  }

  /**
   * Gets modification time.
   *
   * @return the modification time (in milliseconds)
   */
  public String getModificationTime() {
    return CommonUtils.convertMsToDate(mLastModificationTimeMs,
        mAlluxioConfiguration.get(PropertyKey.USER_DATE_FORMAT_PATTERN));
  }

  /**
   * Gets file locations.
   *
   * @return the file locations
   */
  public List<String> getFileLocations() {
    return mFileLocations;
  }

  /**
   * Gets id.
   *
   * @return the file id
   */
  public long getId() {
    return mId;
  }

  /**
   * Gets in alluxio.
   *
   * @return whether the file is present in memory
   */
  public boolean getInAlluxio() {
    return mInAlluxio;
  }

  /**
   * Gets in alluxio percentage.
   *
   * @return the percentage of the file present in memory
   */
  public int getInAlluxioPercentage() {
    return mInAlluxioPercentage;
  }

  /**
   * Gets is directory.
   *
   * @return whether the object represents a directory
   */
  public boolean getIsDirectory() {
    return mIsDirectory;
  }

  /**
   * Is pinned boolean.
   *
   * @return whether the file is pinned
   */
  public boolean isPinned() {
    return mPinned;
  }

  /**
   * Gets persistence state.
   *
   * @return the {@link PersistenceState} of the file
   */
  public String getPersistenceState() {
    return mPersistenceState;
  }

  /**
   * Gets on-tier percentages.
   *
   * @return the on-tier percentages
   */
  public Map<String, Integer> getOnTierPercentages() {
    return mOrderedTierAliases.stream().collect(Collectors.toMap(tier -> tier, tier -> {
      Long sizeOnTier = mSizeOnTier.getOrDefault(tier, 0L);
      return mSize > 0 ? (int) (100 * sizeOnTier / mSize) : 0;
    }));
  }

  /**
   * Gets name.
   *
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
   * Gets size.
   *
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
   * Sets file locations.
   *
   * @param fileLocations the file locations to use
   */
  public void setFileLocations(List<String> fileLocations) {
    mFileLocations.clear();
    mFileLocations.addAll(fileLocations);
  }

  /**
   * Gets owner.
   *
   * @return the owner of the file
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * Gets group.
   *
   * @return the group of the file
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * Gets mode.
   *
   * @return the mode of the file
   */
  public String getMode() {
    return mMode;
  }

  /**
   * Gets BlockId List.
   *
   * @return the BlockId List in worker node
   */
  public List<Long> getBlockIds() {
    return mBlockIds;
  }
}
