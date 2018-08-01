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

import alluxio.Constants;
import alluxio.wire.TtlAction;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting the attributes.
 */
@NotThreadSafe
public final class SetAttributeOptions extends alluxio.file.options.SetAttributeOptions {
  /**
   * @return the default {@link SetAttributeOptions}
   */
  public static SetAttributeOptions defaults() {
    return new SetAttributeOptions();
  }

  /**
   * Constructs a new method option for setting the attributes.
   *
   * @param options the options for setting the attributes
   */
//  public SetAttributeOptions(SetAttributeTOptions options) {
//    this();
//    if (options != null) {
//      if (options.isSetCommonOptions()) {
//        mCommonOptions = new CommonOptions(options.getCommonOptions());
//      }
//      mPinned = options.isSetPinned() ? options.isPinned() : null;
//      mTtl = options.isSetTtl() ? options.getTtl() : null;
//      mTtlAction = ThriftUtils.fromThrift(options.getTtlAction());
//      mPersisted = options.isSetPersisted() ? options.isPersisted() : null;
//      mOwner = options.isSetOwner() ? options.getOwner() : null;
//      mGroup = options.isSetGroup() ? options.getGroup() : null;
//      mMode = options.isSetMode() ? options.getMode() : Constants.INVALID_MODE;
//      mRecursive = options.isRecursive();
//      mOperationTimeMs = System.currentTimeMillis();
//    }
//  }

  private SetAttributeOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
    mPinned = null;
    mTtl = null;
    mTtlAction = TtlAction.DELETE;
    mPersisted = null;
    mOwner = null;
    mGroup = null;
    mMode = Constants.INVALID_MODE;
    mRecursive = false;
    mOperationTimeMs = System.currentTimeMillis();
    mUfsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
  }
}
