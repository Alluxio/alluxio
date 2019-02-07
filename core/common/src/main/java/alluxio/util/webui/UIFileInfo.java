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
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.FileInfo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
   * Provides ordering of {@link alluxio.util.webui.UIFileInfo} based off a string comparison of the
   * absolute paths.
   */
  public static final Ordering<UIFileInfo> PATH_STRING_COMPARE = Ordering.natural()
      .onResultOf((Function<UIFileInfo, Comparable<String>>) input -> input.mAbsolutePath);

  /**
   * The Local file info.
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

  private final Map<String, List<UIFileBlockInfo>> mBlocksOnTier;
  private final Map<String, Long> mSizeOnTier = new HashMap<>();

  /**
   * Creates a new instance of {@link alluxio.util.webui.UIFileInfo}.
   *
   * @param status underlying {@link URIStatus}
   */
  public UIFileInfo(URIStatus status) {
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
    mBlocksOnTier = new HashMap<>();
  }

  /**
   * Instantiates a new Ui file info.
   *
   * @param id the id
   * @param name the name
   * @param absolutePath the absolute path
   * @param blockSizeBytes the block size bytes
   * @param size the size
   * @param creationTimeMs the creation time ms
   * @param lastModificationTimeMs the last modification time ms
   * @param inAlluxio the in alluxio
   * @param inAlluxioPercentage the in alluxio percentage
   * @param isDirectory the is directory
   * @param pinned the pinned
   * @param owner the owner
   * @param group the group
   * @param mode the mode
   * @param persistenceState the persistence state
   * @param fileLocations the file locations
   */
  @JsonCreator
  public UIFileInfo(@JsonProperty("id") long id, @JsonProperty("name") String name,
      @JsonProperty("absolutePath") String absolutePath,
      @JsonProperty("blockSizeBytes") long blockSizeBytes, @JsonProperty("size") long size,
      @JsonProperty("creationTimeMs") long creationTimeMs,
      @JsonProperty("lastModificationTimeMs") long lastModificationTimeMs,
      @JsonProperty("inAlluxio") boolean inAlluxio,
      @JsonProperty("inAlluxioPercentage") int inAlluxioPercentage,
      @JsonProperty("isDirectory") boolean isDirectory, @JsonProperty("pinned") boolean pinned,
      @JsonProperty("owner") String owner, @JsonProperty("group") String group,
      @JsonProperty("mode") String mode, @JsonProperty("persistenceState") String persistenceState,
      @JsonProperty("fileLocations") List<String> fileLocations,
      @JsonProperty("blocksOnTier") Map<String, List<UIFileBlockInfo>> blocksOnTier) {
    mId = id;
    mName = name;
    mAbsolutePath = absolutePath;
    mBlockSizeBytes = blockSizeBytes;
    mSize = size;
    mCreationTimeMs = creationTimeMs;
    mLastModificationTimeMs = lastModificationTimeMs;
    mInAlluxio = inAlluxio;
    mInAlluxioPercentage = inAlluxioPercentage;
    mIsDirectory = isDirectory;
    mPinned = pinned;
    mOwner = owner;
    mGroup = group;
    mMode = mode;
    mPersistenceState = persistenceState;
    mFileLocations = fileLocations;
    mBlocksOnTier = blocksOnTier;
  }

  /**
   * Creates a new instance of {@link alluxio.util.webui.UIFileInfo}.
   *
   * @param info underlying {@link FileInfo}
   */
  public UIFileInfo(FileInfo info) {
    this(new URIStatus(info));
  }

  /**
   * Creates a new instance of {@link alluxio.util.webui.UIFileInfo}.
   *
   * @param fileInfo underlying {@link alluxio.util.webui.UIFileInfo.LocalFileInfo}
   */
  public UIFileInfo(UIFileInfo.LocalFileInfo fileInfo) {
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
    mBlocksOnTier = new HashMap<>();
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
   * @param dateFormatPattern the pattern to use when formatting the timestamp
   * @return the creation time (in milliseconds)
   */
  public String getCreationTime(String dateFormatPattern) {
    if (mCreationTimeMs == UIFileInfo.LocalFileInfo.EMPTY_CREATION_TIME) {
      return "";
    }
    return CommonUtils.convertMsToDate(mCreationTimeMs, dateFormatPattern);
  }

  /**
   * @param dateFormatPattern the pattern to use when formatting the timestamp
   * @return the modification time (in milliseconds)
   */
  public String getModificationTime(String dateFormatPattern) {
    return CommonUtils.convertMsToDate(mLastModificationTimeMs, dateFormatPattern);
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
  public boolean getInAlluxio() {
    return mInAlluxio;
  }

  /**
   * @return the percentage of the file present in memory
   */
  public int getInAlluxioPercentage() {
    return mInAlluxioPercentage;
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
   * Gets owner.
   *
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
