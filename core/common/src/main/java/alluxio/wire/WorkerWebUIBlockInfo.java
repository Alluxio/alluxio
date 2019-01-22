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

import alluxio.util.webui.UIFileBlockInfo;
import alluxio.util.webui.UIFileInfo;

import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.Serializable;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class WorkerWebUIBlockInfo implements Serializable {
  private static final long serialVersionUID = 5596566969362740932L;

  private int mNTotalFile;
  private List<ImmutablePair<String, List<UIFileBlockInfo>>> mFileBlocksOnTier;
  private List<String> mOrderedTierAliases;
  private List<UIFileInfo> mFileInfos;
  private String mBlockSizeBytes;
  private String mFatalError;
  private String mInvalidPathError;
  private String mPath;

  /**
   * Creates a new instance of {@link WorkerWebUIBlockInfo}.
   */
  public WorkerWebUIBlockInfo() {
  }

  /**
   * Gets block size bytes.
   *
   * @return the block size bytes
   */
  public String getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * Gets fatal error.
   *
   * @return the fatal error
   */
  public String getFatalError() {
    return mFatalError;
  }

  /**
   * Gets file blocks on tier.
   *
   * @return the file blocks on tier
   */
  public List<ImmutablePair<String, List<UIFileBlockInfo>>> getFileBlocksOnTier() {
    return mFileBlocksOnTier;
  }

  /**
   * Gets invalid path error.
   *
   * @return the invalid path error
   */
  public String getInvalidPathError() {
    return mInvalidPathError;
  }

  /**
   * Gets n total file.
   *
   * @return the n total file
   */
  public int getNTotalFile() {
    return mNTotalFile;
  }

  /**
   * Gets ordered tier aliases.
   *
   * @return the ordered tier aliases
   */
  public List<String> getOrderedTierAliases() {
    return mOrderedTierAliases;
  }

  /**
   * Gets path.
   *
   * @return the path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Gets file infos.
   *
   * @return the file infos
   */
  public List<UIFileInfo> getFileInfos() {
    return mFileInfos;
  }

  /**
   * Sets block size bytes.
   *
   * @param BlockSizeBytes the block size bytes
   * @return the block size bytes
   */
  public WorkerWebUIBlockInfo setBlockSizeBytes(String BlockSizeBytes) {
    mBlockSizeBytes = BlockSizeBytes;
    return this;
  }

  /**
   * Sets fatal error.
   *
   * @param FatalError the fatal error
   * @return the fatal error
   */
  public WorkerWebUIBlockInfo setFatalError(String FatalError) {
    mFatalError = FatalError;
    return this;
  }

  /**
   * Sets file blocks on tier.
   *
   * @param FileBlocksOnTier the file blocks on tier
   * @return the file blocks on tier
   */
  public WorkerWebUIBlockInfo setFileBlocksOnTier(
      List<ImmutablePair<String, List<UIFileBlockInfo>>> FileBlocksOnTier) {
    mFileBlocksOnTier = FileBlocksOnTier;
    return this;
  }

  /**
   * Sets invalid path error.
   *
   * @param InvalidPathError the invalid path error
   * @return the invalid path error
   */
  public WorkerWebUIBlockInfo setInvalidPathError(String InvalidPathError) {
    mInvalidPathError = InvalidPathError;
    return this;
  }

  /**
   * Sets n total file.
   *
   * @param NTotalFile the n total file
   * @return the n total file
   */
  public WorkerWebUIBlockInfo setNTotalFile(int NTotalFile) {
    mNTotalFile = NTotalFile;
    return this;
  }

  /**
   * Sets ordered tier aliases.
   *
   * @param OrderedTierAliases the ordered tier aliases
   * @return the ordered tier aliases
   */
  public WorkerWebUIBlockInfo setOrderedTierAliases(List<String> OrderedTierAliases) {
    mOrderedTierAliases = OrderedTierAliases;
    return this;
  }

  /**
   * Sets path.
   *
   * @param Path the path
   * @return the path
   */
  public WorkerWebUIBlockInfo setPath(String Path) {
    mPath = Path;
    return this;
  }

  /**
   * Sets file infos.
   *
   * @param FileInfos the file infos
   * @return the file infos
   */
  public WorkerWebUIBlockInfo setFileInfos(List<UIFileInfo> FileInfos) {
    mFileInfos = FileInfos;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("blockSizeBytes", mBlockSizeBytes)
        .add("fatalError", mFatalError).add("fileBlocksOnTier", mFileBlocksOnTier)
        .add("invalidPathError", mInvalidPathError).add("totalFile", mNTotalFile)
        .add("fileInfos", mFileInfos).add("orderedTierAliases", mOrderedTierAliases)
        .add("path", mPath).toString();
  }
}
