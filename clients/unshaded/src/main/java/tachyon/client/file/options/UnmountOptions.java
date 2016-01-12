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

import tachyon.annotation.PublicApi;

/**
 * Method option for unmounting a path.
 */
@PublicApi
public final class UnmountOptions {
  /**
   * @return the default {@link UnmountOptions}
   */
  public static UnmountOptions defaults() {
    return new UnmountOptions();
  }

  /** Whether the data under the mount should be synchronously freed from Tachyon, currently
   * unsupported */
  private boolean mFreeData;

  private UnmountOptions() {
    mFreeData = false;
  }

  /**
   * @return whether to free the data from the mount
   */
  public boolean isFreeData() {
    return mFreeData;
  }

  /**
   * @param freeData the free data flag to set
   * @return the updated options object
   */
  public UnmountOptions setFreeData(boolean freeData) {
    mFreeData = freeData;
    return this;
  }
}
