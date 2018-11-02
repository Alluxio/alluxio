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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Common method options.
 */
@PublicApi
@NotThreadSafe
public final class CommonOptions extends alluxio.file.options.CommonOptions<CommonOptions> {
  private static final long serialVersionUID = -1491370184123698287L;

  /**
   * @return the default {@link alluxio.file.options.CommonOptions}
   */
  public static CommonOptions defaults() {
    return new CommonOptions();
  }

  protected CommonOptions() {
    mSyncIntervalMs = Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL);
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
  }
}
