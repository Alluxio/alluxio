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
 * Method option for mounting a path.
 */
@PublicApi
public final class MountOptions {
  /** Flag for if the mount should be a read-only mount, currently unsupported */
  private boolean mReadOnly;

  /**
   * @return the default {@link MountOptions}
   */
  public static MountOptions defaults() {
    return new MountOptions();
  }

  private MountOptions() {
    mReadOnly = false;
  }

  /**
   * @return whether the read-only flag is set
   */
  public boolean isReadOnly() {
    return mReadOnly;
  }

  /**
   * @param readOnly the read only flag for this option
   * @return the updated options object
   */
  public MountOptions setReadOnly(boolean readOnly) {
    mReadOnly = readOnly;
    return this;
  }
}
