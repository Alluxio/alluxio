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

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;
import alluxio.wire.CommonOptions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import java.util.HashMap;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for mounting a path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class MountOptions extends alluxio.file.options.MountOptions<MountOptions> {
  /**
   * @return the default {@link MountOptions}
   */
  public static MountOptions defaults() {
    return new MountOptions();
  }

  /**
   * Creates a new instance with default values.
   */
  private MountOptions() {
    mCommonOptions = CommonOptions.defaults();
    mReadOnly = false;
    mProperties = new HashMap<>();
    mShared = false;
  }
}
