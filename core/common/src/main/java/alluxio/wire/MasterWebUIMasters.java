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
  private MasterInfo[] mFailedMasterInfos;
  private MasterInfo[] mNormalMasterInfos;
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
   * Get failed master infos master info [ ].
   *
   * @return the master info [ ]
   */
  public MasterInfo[] getFailedMasterInfos() {
    return mFailedMasterInfos;
  }

  /**
   * Get leader master info master info.
   *
   * @return the master info
   */
  public MasterInfo[] getNormalMasterInfos() {
    return mNormalMasterInfos;
  }

  /**
   * Get normal master infos master info [ ].
   *
   * @return the master info [ ]
   */
  public MasterInfo getPrimaryMasterInfo() {
    return mPrimaryMasterInfo;
  }

  /**
   * Sets debug.
   *
   * @param debug the debug
   * @return the debug master infos
   */
  public MasterWebUIMasters setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  /**
   * Sets failed master infos.
   *
   * @param failedMasterInfos the failed master infos
   * @return the failed master infos
   */
  public MasterWebUIMasters setFailedMasterInfos(MasterInfo[] failedMasterInfos) {
    mFailedMasterInfos = failedMasterInfos.clone();
    return this;
  }

  /**
   * Sets normal master infos.
   *
   * @param normalMasterInfos the normal master infos
   * @return the normal master infos
   */
  public MasterWebUIMasters setNormalMasterInfos(MasterInfo[] normalMasterInfos) {
    mNormalMasterInfos = normalMasterInfos.clone();
    return this;
  }

  /**
   * Sets leader master info.
   *
   * @param primaryMasterInfo the normal master info
   * @return the leader master info
   */
  public MasterWebUIMasters setPrimaryMasterInfo(MasterInfo primaryMasterInfo) {
    mPrimaryMasterInfo = primaryMasterInfo;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("debug", mDebug)
        .add("failedMasterInfos", mFailedMasterInfos)
        .add("normalMasterInfos", mNormalMasterInfos)
        .add("primaryMasterInfo", mPrimaryMasterInfo).toString();
  }
}
