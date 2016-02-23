/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for listing the status.
 */
@PublicApi
@NotThreadSafe
public final class ListStatusOptions {
  /** Whether or not to check the ufs if the path does not exist in Alluxio. */
  private boolean mCheckUfs;

  /**
   * @return the default {@link ListStatusOptions}
   */
  public static ListStatusOptions defaults() {
    return new ListStatusOptions();
  }

  private ListStatusOptions() {
    mCheckUfs = true;
  }

  /**
   * @return whether we should sync the under file system for the file if it is not found in
   *         Alluxio space
   */
  public boolean isCheckUfs() {
    return mCheckUfs;
  }

  /**
   * @param checkUfs the check ufs flag to set
   * @return the updated options object
   */
  public ListStatusOptions setCheckUfs(boolean checkUfs) {
    mCheckUfs = checkUfs;
    return this;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ListStatus(");
    sb.append(super.toString()).append(", Check UFS: ").append(mCheckUfs);
    sb.append(")");
    return sb.toString();
  }
}
