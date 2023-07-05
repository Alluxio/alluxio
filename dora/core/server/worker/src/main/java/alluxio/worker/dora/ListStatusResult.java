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

package alluxio.worker.dora;

import alluxio.underfs.UfsStatus;

/**
 * The list status results stored in the cache.
 */
public class ListStatusResult {
  long mTimeStamp;
  UfsStatus[] mUfsStatuses;

  private final boolean mIsFile;

  /**
   * @return if the list target is a file; if true,
   * the ufs status array will only contain one element, being the file metadata itself.
   */
  boolean isFile() {
    return mIsFile;
  }

  ListStatusResult(long timeStamp, UfsStatus[] ufsStatuses, boolean isFile) {
    mTimeStamp = timeStamp;
    mUfsStatuses = ufsStatuses;
    mIsFile = isFile;
  }
}
