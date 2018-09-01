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
import alluxio.underfs.UfsMode;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for updating operation mode of a ufs path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class UpdateUfsModeOptions extends alluxio.file.options.UpdateUfsModeOptions {
  /**
   * @return the default {@link UpdateUfsModeOptions}
   */
  public static UpdateUfsModeOptions defaults() {
    return new UpdateUfsModeOptions();
  }

  private UpdateUfsModeOptions() {
    mUfsMode = UfsMode.READ_WRITE;
  }
}
