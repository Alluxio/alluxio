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

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for getting the status of a path.
 */
@PublicApi
@NotThreadSafe
public final class GetStatusOptions {
  /** Whether or not to check the ufs if the path does not exist in Alluxio. */
  private boolean mCheckUfs;

  /**
   * @return a default {@link GetStatusOptions} based on the client's configuration
   */
  public static GetStatusOptions defaults() {
    return new GetStatusOptions();
  }

  private GetStatusOptions() {
    // TODO(calvin): Make this configurable
    mCheckUfs = false;
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
  public GetStatusOptions setCheckUfs(boolean checkUfs) {
    mCheckUfs = checkUfs;
    return this;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("GetStatus(");
    sb.append(super.toString()).append(", Check UFS: ").append(mCheckUfs);
    sb.append(")");
    return sb.toString();
  }
}
