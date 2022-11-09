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

import alluxio.util.CommonUtils;

/**
 * Displays information about a worker in the UI.
 */
public class UIMasterInfo {
  private final String mMasterAddress;
  private final long mId;
  private final long mLastUpdatedTimeMs;

  /**
   * Creates a new instance of {@link UIMasterInfo}.
   *
   * @param masterAddress The master address
   * @param id The master id
   * @param lastUpdatedTimeMs The last heart beat in ms
   */
  public UIMasterInfo(String masterAddress, long id, long lastUpdatedTimeMs) {
    mMasterAddress = masterAddress;
    mId = id;
    mLastUpdatedTimeMs = lastUpdatedTimeMs;
  }

  /**
   * Get master address.
   *
   * @return the master address
   */
  public String getAddress() {
    return mMasterAddress;
  }

  /**
   * Get id.
   *
   * @return the id
   */
  public String getId() {
    return Long.toString(mId);
  }

  /**
   * Get master last update time.
   *
   * @return the master last update time
   */
  public String getLastUpdatedTime() {
    return CommonUtils.convertMsToClockTime(mLastUpdatedTimeMs);
  }
}
