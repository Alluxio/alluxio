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

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI masters information.
 */
@NotThreadSafe
public final class MasterWebUIMasters implements Serializable {
  private static final long serialVersionUID = -2709466215687255197L;

  private boolean mDebug;
  private MasterInfo[] mLostMasterInfos;
  private MasterInfo[] mStandbyMasterInfos;
  private MasterInfo mPrimaryMasterInfo;

  /**
   * Creates a new instance of {@link MasterWebUIMasters}.
   */
  public MasterWebUIMasters() {
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
   * Get info of lost masters.
   *
   * @return an array of lost {@link MasterInfo}
   */
  public MasterInfo[] getLostMasterInfos() {
    return mLostMasterInfos;
  }

  /**
   * Get info of standby masters.
   *
   * @return an array of standby {@link MasterInfo}
   */
  public MasterInfo[] getStandbyMasterInfos() {
    return mStandbyMasterInfos;
  }

  /**
   * Get info of the primary master.
   *
   * @return the primary {@link MasterInfo}
   */
  public MasterInfo getPrimaryMasterInfo() {
    return mPrimaryMasterInfo;
  }

  /**
   * Sets debug.
   *
   * @param debug the debug
   * @return the {@link MasterWebUIMasters} instance
   */
  public MasterWebUIMasters setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  /**
   * Sets lost master infos.
   *
   * @param lostMasterInfos an array of lost {@link MasterInfo}
   * @return the {@link MasterWebUIMasters} instance
   */
  public MasterWebUIMasters setLostMasterInfos(MasterInfo[] lostMasterInfos) {
    mLostMasterInfos = lostMasterInfos.clone();
    return this;
  }

  /**
   * Sets standby master infos.
   *
   * @param standbyMasterInfos an array of standby {@link MasterInfo}
   * @return the {@link MasterWebUIMasters} instance
   */
  public MasterWebUIMasters setStandbyMasterInfos(MasterInfo[] standbyMasterInfos) {
    mStandbyMasterInfos = standbyMasterInfos.clone();
    return this;
  }

  /**
   * Sets primary master info.
   *
   * @param primaryMasterInfo the primary {@link MasterInfo}
   * @return the {@link MasterWebUIMasters} instance
   */
  public MasterWebUIMasters setPrimaryMasterInfo(MasterInfo primaryMasterInfo) {
    mPrimaryMasterInfo = primaryMasterInfo;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("debug", mDebug)
        .add("lostMasterInfos", mLostMasterInfos)
        .add("standbyMasterInfos", mStandbyMasterInfos)
        .add("primaryMasterInfo", mPrimaryMasterInfo).toString();
  }
}
