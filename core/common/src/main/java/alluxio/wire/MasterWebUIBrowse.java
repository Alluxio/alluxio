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

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI browse information.
 */
@NotThreadSafe
public final class MasterWebUIBrowse implements Serializable {
  private static final long serialVersionUID = 5446587832759273932L;

  private boolean mDebug;
  private boolean mShowPermissions;
  private int mNTotalFile;
  private List<UIFileBlockInfo> mFileBlocks;
  private List<UIFileInfo> mFileInfos;
  private long mViewingOffset;
  private String mAccessControlException;
  private String mBlockSizeBytes;
  private String mCurrentPath;
  private String mFatalError;
  private String mFileData;
  private String mFileDoesNotExistException;
  private String mHighestTierAlias;
  private String mInvalidPathError;
  private String mInvalidPathException;
  private String mMasterNodeAddress;
  private UIFileInfo mCurrentDirectory;
  private UIFileInfo[] mPathInfos;

  /**
   * Creates a new instance of {@link MasterWebUIBrowse}.
   */
  public MasterWebUIBrowse() {
  }

  /**
   * Gets access control exception.
   *
   * @return the access control exception
   */
  public String getAccessControlException() {
    return mAccessControlException;
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
   * Gets current directory.
   *
   * @return the current directory
   */
  public UIFileInfo getCurrentDirectory() {
    return mCurrentDirectory;
  }

  /**
   * Gets current path.
   *
   * @return the current path
   */
  public String getCurrentPath() {
    return mCurrentPath;
  }

  /**
   * Gets debug.
   *
   * @return the debug
   */
  public boolean getDebug() {
    return mDebug;
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
   * Gets file blocks.
   *
   * @return the file blocks
   */
  public List<UIFileBlockInfo> getFileBlocks() {
    return mFileBlocks;
  }

  /**
   * Gets file data.
   *
   * @return the file data
   */
  public String getFileData() {
    return mFileData;
  }

  /**
   * Gets file does not exist exception.
   *
   * @return the file does not exist exception
   */
  public String getFileDoesNotExistException() {
    return mFileDoesNotExistException;
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
   * Gets highest tier alias.
   *
   * @return the highest tier alias
   */
  public String getHighestTierAlias() {
    return mHighestTierAlias;
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
   * Gets invalid path exception.
   *
   * @return the invalid path exception
   */
  public String getInvalidPathException() {
    return mInvalidPathException;
  }

  /**
   * Gets master node address.
   *
   * @return the master node address
   */
  public String getMasterNodeAddress() {
    return mMasterNodeAddress;
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
   * Get path infos ui file info [ ].
   *
   * @return the ui file info [ ]
   */
  public UIFileInfo[] getPathInfos() {
    return mPathInfos;
  }

  /**
   * Gets show permissions.
   *
   * @return the show permissions
   */
  public boolean getShowPermissions() {
    return mShowPermissions;
  }

  /**
   * Gets viewing offset.
   *
   * @return the viewing offset
   */
  public long getViewingOffset() {
    return mViewingOffset;
  }

  /**
   * Sets access control exception.
   *
   * @param accessControlException the access control exception
   * @return the access control exception
   */
  public MasterWebUIBrowse setAccessControlException(String accessControlException) {
    mAccessControlException = accessControlException;
    return this;
  }

  /**
   * Sets block size bytes.
   *
   * @param blockSizeBytes the block size bytes
   * @return the block size bytes
   */
  public MasterWebUIBrowse setBlockSizeBytes(String blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * Sets current directory.
   *
   * @param currentDirectory the current directory
   * @return the current directory
   */
  public MasterWebUIBrowse setCurrentDirectory(UIFileInfo currentDirectory) {
    mCurrentDirectory = currentDirectory;
    return this;
  }

  /**
   * Sets current path.
   *
   * @param currentPath the current path
   * @return the current path
   */
  public MasterWebUIBrowse setCurrentPath(String currentPath) {
    mCurrentPath = currentPath;
    return this;
  }

  /**
   * Sets debug.
   *
   * @param debug the debug
   * @return the debug
   */
  public MasterWebUIBrowse setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  /**
   * Sets fatal error.
   *
   * @param fatalError the fatal error
   * @return the fatal error
   */
  public MasterWebUIBrowse setFatalError(String fatalError) {
    mFatalError = fatalError;
    return this;
  }

  /**
   * Sets file blocks.
   *
   * @param fileBlocks the file blocks
   * @return the file blocks
   */
  public MasterWebUIBrowse setFileBlocks(List<UIFileBlockInfo> fileBlocks) {
    mFileBlocks = fileBlocks;
    return this;
  }

  /**
   * Sets file data.
   *
   * @param fileData the file data
   * @return the file data
   */
  public MasterWebUIBrowse setFileData(String fileData) {
    mFileData = fileData;
    return this;
  }

  /**
   * Sets file does not exist exception.
   *
   * @param fileDoesNotExistException the file does not exist exception
   * @return the file does not exist exception
   */
  public MasterWebUIBrowse setFileDoesNotExistException(String fileDoesNotExistException) {
    mFileDoesNotExistException = fileDoesNotExistException;
    return this;
  }

  /**
   * Sets file infos.
   *
   * @param fileInfos the file infos
   * @return the file infos
   */
  public MasterWebUIBrowse setFileInfos(List<UIFileInfo> fileInfos) {
    mFileInfos = fileInfos;
    return this;
  }

  /**
   * Sets highest tier alias.
   *
   * @param highestTierAlias the highest tier alias
   * @return the highest tier alias
   */
  public MasterWebUIBrowse setHighestTierAlias(String highestTierAlias) {
    mHighestTierAlias = highestTierAlias;
    return this;
  }

  /**
   * Sets invalid path error.
   *
   * @param invalidPathError the invalid path error
   * @return the invalid path error
   */
  public MasterWebUIBrowse setInvalidPathError(String invalidPathError) {
    mInvalidPathError = invalidPathError;
    return this;
  }

  /**
   * Sets invalid path exception.
   *
   * @param invalidPathException the invalid path exception
   * @return the invalid path exception
   */
  public MasterWebUIBrowse setInvalidPathException(String invalidPathException) {
    mInvalidPathException = invalidPathException;
    return this;
  }

  /**
   * Sets master node address.
   *
   * @param masterNodeAddress the master node address
   * @return the master node address
   */
  public MasterWebUIBrowse setMasterNodeAddress(String masterNodeAddress) {
    mMasterNodeAddress = masterNodeAddress;
    return this;
  }

  /**
   * Sets n total file.
   *
   * @param nTotalFile the n total file
   * @return the n total file
   */
  public MasterWebUIBrowse setNTotalFile(int nTotalFile) {
    mNTotalFile = nTotalFile;
    return this;
  }

  /**
   * Sets path infos.
   *
   * @param pathInfos the path infos
   * @return the path infos
   */
  public MasterWebUIBrowse setPathInfos(UIFileInfo[] pathInfos) {
    mPathInfos = pathInfos.clone();
    return this;
  }

  /**
   * Sets show permissions.
   *
   * @param showPermissions the show permissions
   * @return the show permissions
   */
  public MasterWebUIBrowse setShowPermissions(boolean showPermissions) {
    mShowPermissions = showPermissions;
    return this;
  }

  /**
   * Sets viewing offset.
   *
   * @param viewingOffset the viewing offset
   * @return the viewing offset
   */
  public MasterWebUIBrowse setViewingOffset(long viewingOffset) {
    mViewingOffset = viewingOffset;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("accessControlException", mAccessControlException)
        .add("blockSizeBytes", mBlockSizeBytes).add("currentDirectory", mCurrentDirectory)
        .add("currentPath", mCurrentPath).add("debug", mDebug).add("fatalError", mFatalError)
        .add("fileBlocks", mFileBlocks).add("fileData", mFileData)
        .add("fileDoesNotExistException", mFileDoesNotExistException).add("fileInfos", mFileInfos)
        .add("highestTierAlias", mHighestTierAlias).add("invalidPathError", mInvalidPathError)
        .add("invalidPathException", mInvalidPathException)
        .add("masterNodeAddress", mMasterNodeAddress).add("nTotalFile", mNTotalFile)
        .add("pathInfos", mPathInfos).add("showPermissions", mShowPermissions)
        .add("viewingOffset", mViewingOffset).toString();
  }
}
