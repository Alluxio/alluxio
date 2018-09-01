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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for renaming a file or a directory.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class RenameOptions extends alluxio.file.options.RenameOptions {
  /**
   * @return the default {@link RenameOptions}
   */
  public static RenameOptions defaults() {
    return new RenameOptions();
  }

  private RenameOptions() {
    mCommonOptions = CommonOptions.defaults();
  }
}
