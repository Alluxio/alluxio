/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.file.options;

import javax.annotation.concurrent.NotThreadSafe;

import tachyon.annotation.PublicApi;

/**
 * Method option for listing the status.
 */
@PublicApi
@NotThreadSafe
public final class ListStatusOptions {
  /** Whether or not to check the ufs if the path does not exist in Tachyon */
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
   *         Tachyon space
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
}
