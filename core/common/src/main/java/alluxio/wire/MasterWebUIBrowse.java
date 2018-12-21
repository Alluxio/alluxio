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

import alluxio.util.webui.UIFileInfo;
import alluxio.util.webui.UIFileBlockInfo;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI browse information.
 */
@NotThreadSafe
public final class MasterWebUIBrowse implements Serializable {
  private String mAccessControlException;
  private String mBlockSizeBytes;
  private UIFileInfo mCurrentDirectory;
  private String mCurrentPath;
  private boolean mDebug;
  private String mFatalError;
  private List<UIFileBlockInfo> mFileBlocks;
  private String mFileData;
  private String mFileDoesNotExistException;
  private List<UIFileInfo> mFileInfos;
  private String mHighestTierAlias;
  private String mInvalidPathError;
  private String mInvalidPathException;
  private String mMasterNodeAddress;
  private int mNTotalFile;
  private UIFileInfo[] mPathInfos;
  private boolean mShowPermissions;
  private long mViewingOffset;

  /**
   * Creates a new instance of {@link MasterWebUIBrowse}.
   */
  public MasterWebUIBrowse() {
  }

  public String getAccessControlException() {
    return mAccessControlException;
  }

  public String getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  public UIFileInfo getCurrentDirectory() {
    return mCurrentDirectory;
  }

  public String getCurrentPath() {
    return mCurrentPath;
  }

  public boolean getDebug() {
    return mDebug;
  }

  public String getFatalError() {
    return mFatalError;
  }

  public List<UIFileBlockInfo> getFileBlocks() {
    return mFileBlocks;
  }

  public String getFileData() {
    return mFileData;
  }

  public String getFileDoesNotExistException() {
    return mFileDoesNotExistException;
  }

  public List<UIFileInfo> getFileInfos() {
    return mFileInfos;
  }

  public String getHighestTierAlias() {
    return mHighestTierAlias;
  }

  public String getInvalidPathError() {
    return mInvalidPathError;
  }

  public String getInvalidPathException() {
    return mInvalidPathException;
  }

  public String getMasterNodeAddress() {
    return mMasterNodeAddress;
  }

  public int getNTotalFile() {
    return mNTotalFile;
  }

  public UIFileInfo[] getPathInfos() {
    return mPathInfos;
  }

  public boolean getShowPermissions() {
    return mShowPermissions;
  }

  public long getViewingOffset() {
    return mViewingOffset;
  }

  public MasterWebUIBrowse setAccessControlException(String accessControlException) {
    mAccessControlException = accessControlException;
    return this;
  }

  public MasterWebUIBrowse setBlockSizeBytes(String blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  public MasterWebUIBrowse setCurrentDirectory(UIFileInfo currentDirectory) {
    mCurrentDirectory = currentDirectory;
    return this;
  }

  public MasterWebUIBrowse setCurrentPath(String currentPath) {
    mCurrentPath = currentPath;
    return this;
  }

  public MasterWebUIBrowse setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  public MasterWebUIBrowse setFatalError(String fatalError) {
    mFatalError = fatalError;
    return this;
  }

  public MasterWebUIBrowse setFileBlocks(List<UIFileBlockInfo> fileBlocks) {
    mFileBlocks = fileBlocks;
    return this;
  }

  public MasterWebUIBrowse setFileData(String fileData) {
    mFileData = fileData;
    return this;
  }

  public MasterWebUIBrowse setFileDoesNotExistException(String fileDoesNotExistException) {
    mFileDoesNotExistException = fileDoesNotExistException;
    return this;
  }

  public MasterWebUIBrowse setFileInfos(List<UIFileInfo> fileInfos) {
    mFileInfos = fileInfos;
    return this;
  }

  public MasterWebUIBrowse setHighestTierAlias(String highestTierAlias) {
    mHighestTierAlias = highestTierAlias;
    return this;
  }

  public MasterWebUIBrowse setInvalidPathError(String invalidPathError) {
    mInvalidPathError = invalidPathError;
    return this;
  }

  public MasterWebUIBrowse setInvalidPathException(String invalidPathException) {
    mInvalidPathException = invalidPathException;
    return this;
  }

  public MasterWebUIBrowse setMasterNodeAddress(String masterNodeAddress) {
    mMasterNodeAddress = masterNodeAddress;
    return this;
  }

  public MasterWebUIBrowse setNTotalFile(int nTotalFile) {
    mNTotalFile = nTotalFile;
    return this;
  }

  public MasterWebUIBrowse setPathInfos(UIFileInfo[] pathInfos) {
    mPathInfos = pathInfos;
    return this;
  }

  public MasterWebUIBrowse setShowPermissions(boolean showPermissions) {
    mShowPermissions = showPermissions;
    return this;
  }

  public MasterWebUIBrowse setViewingOffset(long viewingOffset) {
    mViewingOffset = viewingOffset;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("mAccessControlException", mAccessControlException)
        .add("mBlockSizeBytes", mBlockSizeBytes).add("mCurrentDirectory", mCurrentDirectory)
        .add("mCurrentPath", mCurrentPath).add("mDebug", mDebug).add("mFatalError", mFatalError)
        .add("mFileBlocks", mFileBlocks).add("mFileData", mFileData)
        .add("mFileDoesNotExistException", mFileDoesNotExistException).add("mFileInfos", mFileInfos)
        .add("mHighestTierAlias", mHighestTierAlias).add("mInvalidPathError", mInvalidPathError)
        .add("mInvalidPathException", mInvalidPathException)
        .add("mMasterNodeAddress", mMasterNodeAddress).add("mNTotalFile", mNTotalFile)
        .add("mPathInfos", mPathInfos).add("mShowPermissions", mShowPermissions)
        .add("mViewingOffset", mViewingOffset).toString();
  }
}
