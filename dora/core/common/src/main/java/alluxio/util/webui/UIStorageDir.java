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

/**
 * Displays information about a storage directory in the UI.
 */
public class UIStorageDir {
  private final String mTierAlias;
  private final String mDirPath;
  private final long mCapacityBytes;
  private final long mUsedBytes;

  /**
   * Creates a new instance of {@link UIStorageDir}.
   *
   * @param tierAlias tier alias
   * @param dirPath directory path
   * @param capacityBytes capacity in bytes
   * @param usedBytes used capacity in bytes
   */
  public UIStorageDir(String tierAlias, String dirPath, long capacityBytes, long usedBytes) {
    mTierAlias = tierAlias;
    mDirPath = dirPath;
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
  }

  /**
   * Gets capacity bytes.
   *
   * @return capacity in bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * Gets dir path.
   *
   * @return directory path
   */
  public String getDirPath() {
    return mDirPath;
  }

  /**
   * Gets tier alias.
   *
   * @return tier alias
   */
  public String getTierAlias() {
    return mTierAlias;
  }

  /**
   * Gets used bytes.
   *
   * @return used capacity in bytes
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }
}
