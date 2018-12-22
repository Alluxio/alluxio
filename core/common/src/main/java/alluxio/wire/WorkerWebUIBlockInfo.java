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

import com.google.common.base.Objects;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.Serializable;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class WorkerWebUIBlockInfo implements Serializable {
  private String mBlockSizeBytes;
  private String mFatalError;
  private List<ImmutablePair<String, List<UIFileBlockInfo>>> mFileBlocksOnTier;
  private List<UIFileInfo> mFileInfos;
  private String mInvalidPathError;
  private int mNTotalFile;
  private List<String> mOrderedTierAliases;
  private String mPath;

  /**
   * Creates a new instance of {@link WorkerWebUIBlockInfo}.
   */
  public WorkerWebUIBlockInfo() {
  }

  public String getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  public String getFatalError() {
    return mFatalError;
  }

  public List<ImmutablePair<String, List<UIFileBlockInfo>>> getFileBlocksOnTier() {
    return mFileBlocksOnTier;
  }

  public String getInvalidPathError() {
    return mInvalidPathError;
  }

  public int getNTotalFile() {
    return mNTotalFile;
  }

  public List<String> getOrderedTierAliases() {
    return mOrderedTierAliases;
  }

  public String getPath() {
    return mPath;
  }

  public List<UIFileInfo> getFileInfos() {
    return mFileInfos;
  }

  public WorkerWebUIBlockInfo setBlockSizeBytes(String BlockSizeBytes) {
    mBlockSizeBytes = BlockSizeBytes;
    return this;
  }

  public WorkerWebUIBlockInfo setFatalError(String FatalError) {
    mFatalError = FatalError;
    return this;
  }

  public WorkerWebUIBlockInfo setFileBlocksOnTier(
      List<ImmutablePair<String, List<UIFileBlockInfo>>> FileBlocksOnTier) {
    mFileBlocksOnTier = FileBlocksOnTier;
    return this;
  }

  public WorkerWebUIBlockInfo setInvalidPathError(String InvalidPathError) {
    mInvalidPathError = InvalidPathError;
    return this;
  }

  public WorkerWebUIBlockInfo setNTotalFile(int NTotalFile) {
    mNTotalFile = NTotalFile;
    return this;
  }

  public WorkerWebUIBlockInfo setOrderedTierAliases(List<String> OrderedTierAliases) {
    mOrderedTierAliases = OrderedTierAliases;
    return this;
  }

  public WorkerWebUIBlockInfo setPath(String Path) {
    mPath = Path;
    return this;
  }

  public WorkerWebUIBlockInfo setFileInfos(List<UIFileInfo> FileInfos) {
    mFileInfos = FileInfos;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("mBlockSizeBytes", mBlockSizeBytes)
        .add("mFatalError", mFatalError).add("mFileBlocksOnTier", mFileBlocksOnTier)
        .add("mInvalidPathError", mInvalidPathError).add("mNTotalFile", mNTotalFile)
        .add("mFileInfos", mFileInfos).add("mOrderedTierAliases", mOrderedTierAliases)
        .add("mPath", mPath).toString();
  }
}
