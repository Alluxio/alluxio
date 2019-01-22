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

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI logs information.
 */
@NotThreadSafe
public final class MasterWebUILogs implements Serializable {
  private static final long serialVersionUID = -122516271374678772L;

  private boolean mDebug;
  private int mNTotalFile;
  private List<UIFileInfo> mFileInfos;
  private long mViewingOffset;
  private String mCurrentPath;
  private String mFatalError;
  private String mFileData;
  private String mInvalidPathError;

  /**
   * Creates a new instance of {@link MasterWebUIWorkers}.
   */
  public MasterWebUILogs() {
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
   * Gets file data.
   *
   * @return the file data
   */
  public String getFileData() {
    return mFileData;
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
   * Gets viewing offset.
   *
   * @return the viewing offset
   */
  public long getViewingOffset() {
    return mViewingOffset;
  }

  /**
   * Sets current path.
   *
   * @param currentPath the current path
   * @return the current path
   */
  public MasterWebUILogs setCurrentPath(String currentPath) {
    mCurrentPath = currentPath;
    return this;
  }

  /**
   * Sets debug.
   *
   * @param debug the debug
   * @return the debug
   */
  public MasterWebUILogs setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  /**
   * Sets fatal error.
   *
   * @param fatalError the fatal error
   * @return the fatal error
   */
  public MasterWebUILogs setFatalError(String fatalError) {
    mFatalError = fatalError;
    return this;
  }

  /**
   * Sets file data.
   *
   * @param fileData the file data
   * @return the file data
   */
  public MasterWebUILogs setFileData(String fileData) {
    mFileData = fileData;
    return this;
  }

  /**
   * Sets file infos.
   *
   * @param fileInfos the file infos
   * @return the file infos
   */
  public MasterWebUILogs setFileInfos(List<UIFileInfo> fileInfos) {
    mFileInfos = fileInfos;
    return this;
  }

  /**
   * Sets invalid path error.
   *
   * @param invalidPathError the invalid path error
   * @return the invalid path error
   */
  public MasterWebUILogs setInvalidPathError(String invalidPathError) {
    mInvalidPathError = invalidPathError;
    return this;
  }

  /**
   * Sets n total file.
   *
   * @param nTotalFile the n total file
   * @return the n total file
   */
  public MasterWebUILogs setNTotalFile(int nTotalFile) {
    mNTotalFile = nTotalFile;
    return this;
  }

  /**
   * Sets viewing offset.
   *
   * @param viewingOffset the viewing offset
   * @return the viewing offset
   */
  public MasterWebUILogs setViewingOffset(long viewingOffset) {
    mViewingOffset = viewingOffset;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("currentPath", mCurrentPath).add("debug", mDebug)
        .add("fatalError", mFatalError).add("fileData", mFileData).add("fileInfos", mFileInfos)
        .add("invalidPathError", mInvalidPathError).add("nTotalFile", mNTotalFile)
        .add("viewingOffset", mViewingOffset).toString();
  }
}
