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

import alluxio.wire.CommonOptions;
import alluxio.wire.LoadMetadataType;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for getStatus.
 */
@NotThreadSafe
public final class GetStatusOptions
    extends alluxio.file.options.GetStatusOptions<GetStatusOptions> {
  /**
   * @return the default {@link GetStatusOptions}
   */
  public static GetStatusOptions defaults() {
    return new GetStatusOptions();
  }

  private GetStatusOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
    mLoadMetadataType = LoadMetadataType.Once;
  }
}
