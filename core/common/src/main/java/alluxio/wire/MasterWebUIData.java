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
 * Alluxio WebUI memory information.
 */
@NotThreadSafe
public final class MasterWebUIData implements Serializable {
  private static final long serialVersionUID = 6230017471420697680L;

  private boolean mShowPermissions;
  private int mInAlluxioFileNum;
  private List<UIFileInfo> mFileInfos;
  private String mFatalError;
  private String mMasterNodeAddress;
  private String mPermissionError;

  /**
   * Creates a new instance of {@link MasterWebUIData}.
   */
  public MasterWebUIData() {
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
   * Gets file infos.
   *
   * @return the file infos
   */
  public List<UIFileInfo> getFileInfos() {
    return mFileInfos;
  }

  /**
   * Gets in alluxio file num.
   *
   * @return the in alluxio file num
   */
  public int getInAlluxioFileNum() {
    return mInAlluxioFileNum;
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
   * Gets permission error.
   *
   * @return the permission error
   */
  public String getPermissionError() {
    return mPermissionError;
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
   * Sets fatal error.
   *
   * @param FatalError the fatal error
   * @return the fatal error
   */
  public MasterWebUIData setFatalError(String FatalError) {
    mFatalError = FatalError;
    return this;
  }

  /**
   * Sets file infos.
   *
   * @param FileInfos the file infos
   * @return the file infos
   */
  public MasterWebUIData setFileInfos(List<UIFileInfo> FileInfos) {
    mFileInfos = FileInfos;
    return this;
  }

  /**
   * Sets in alluxio file num.
   *
   * @param InAlluxioFileNum the in alluxio file num
   * @return the in alluxio file num
   */
  public MasterWebUIData setInAlluxioFileNum(int InAlluxioFileNum) {
    mInAlluxioFileNum = InAlluxioFileNum;
    return this;
  }

  /**
   * Sets master node address.
   *
   * @param MasterNodeAddress the master node address
   * @return the master node address
   */
  public MasterWebUIData setMasterNodeAddress(String MasterNodeAddress) {
    mMasterNodeAddress = MasterNodeAddress;
    return this;
  }

  /**
   * Sets permission error.
   *
   * @param PermissionError the permission error
   * @return the permission error
   */
  public MasterWebUIData setPermissionError(String PermissionError) {
    mPermissionError = PermissionError;
    return this;
  }

  /**
   * Sets show permissions.
   *
   * @param ShowPermissions the show permissions
   * @return the show permissions
   */
  public MasterWebUIData setShowPermissions(boolean ShowPermissions) {
    mShowPermissions = ShowPermissions;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("fatalError", mFatalError)
        .add("fileInfos", mFileInfos).add("inAlluxioFileNum", mInAlluxioFileNum)
        .add("masterNodeAddress", mMasterNodeAddress).add("permissionError", mPermissionError)
        .add("showPermissions", mShowPermissions).toString();
  }
}
