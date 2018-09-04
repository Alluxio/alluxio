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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;
import alluxio.wire.TtlAction;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions extends alluxio.file.options.CreateFileOptions {
  /**
   * @return the default {@link CreateFileOptions}
   */
  public static CreateFileOptions defaults() {
    return new CreateFileOptions();
  }

  private CreateFileOptions() {
    super();

    // TODO(adit): redundant definition in CreateFileOptions
    mCommonOptions = CommonOptions.defaults();
    mMountPoint = false;
    mOperationTimeMs = System.currentTimeMillis();
    mOwner = "";
    mGroup = "";
    mMode = Mode.defaults();
    mPersisted = false;
    mRecursive = false;
    mMetadataLoad = false;

    mBlockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
    mMode = ModeUtils.applyFileUMask(mMode);
    mCacheable = false;
  }
}
