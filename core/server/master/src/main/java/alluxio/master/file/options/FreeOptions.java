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

package alluxio.master.file.options;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for free.
 */
@NotThreadSafe
public final class FreeOptions extends alluxio.file.options.FreeOptions {
  /**
   * @return the default {@link FreeOptions}
   */
  public static FreeOptions defaults() {
    return new FreeOptions();
  }

  private FreeOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
    mForced = false;
    mRecursive = false;
  }
}
