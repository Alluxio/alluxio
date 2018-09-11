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
import alluxio.wire.TtlAction;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting any number of a path's attributes. If a value is set as null, it
 * will be interpreted as an unset value and the current value will be unchanged.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class SetAttributeOptions
    extends alluxio.file.options.SetAttributeOptions<SetAttributeOptions> {
  /**
   * @return the default {@link SetAttributeOptions}
   */
  public static SetAttributeOptions defaults() {
    return new SetAttributeOptions();
  }

  private SetAttributeOptions() {
    mCommonOptions = CommonOptions.defaults();
    mPinned = null;
    mTtl = null;
    mTtlAction = TtlAction.DELETE;
    mPersisted = null;
    mOwner = null;
    mGroup = null;
    mMode = null;
    mRecursive = false;
  }
}
