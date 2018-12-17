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

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI memory information.
 */
@NotThreadSafe
public final class WebUIData implements Serializable {
  private String mFatalError;
  private List<UIFileInfo> mFileInfos;
  private int mInAlluxioFileNum;
  private String mMasterNodeAddress;
  private String mPermissionError;
  private boolean mShowPermissions;

  /**
   * Creates a new instance of {@link WebUIData}.
   */
  public WebUIData() {
  }

  public String getFatalError() {
    return mFatalError;
  }

  public List<UIFileInfo> getFileInfos() {
    return mFileInfos;
  }

  public int getInAlluxioFileNum() {
    return mInAlluxioFileNum;
  }

  public String getMasterNodeAddress() {
    return mMasterNodeAddress;
  }

  public String getPermissionError() {
    return mPermissionError;
  }

  public boolean getShowPermissions() {
    return mShowPermissions;
  }

  public WebUIData setFatalError(String FatalError) {
    mFatalError = FatalError;
    return this;
  }

  public WebUIData setFileInfos(List<UIFileInfo> FileInfos) {
    mFileInfos = FileInfos;
    return this;
  }

  public WebUIData setInAlluxioFileNum(int InAlluxioFileNum) {
    mInAlluxioFileNum = InAlluxioFileNum;
    return this;
  }

  public WebUIData setMasterNodeAddress(String MasterNodeAddress) {
    mMasterNodeAddress = MasterNodeAddress;
    return this;
  }

  public WebUIData setPermissionError(String PermissionError) {
    mPermissionError = PermissionError;
    return this;
  }

  public WebUIData setShowPermissions(boolean ShowPermissions) {
    mShowPermissions = ShowPermissions;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("mFatalError", mFatalError)
        .add("mFileInfos", mFileInfos).add("mInAlluxioFileNum", mInAlluxioFileNum)
        .add("mMasterNodeAddress", mMasterNodeAddress).add("mPermissionError", mPermissionError)
        .add("mShowPermissions", mShowPermissions).toString();
  }
}
