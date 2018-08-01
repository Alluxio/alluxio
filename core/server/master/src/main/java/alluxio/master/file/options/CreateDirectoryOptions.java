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
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.util.ModeUtils;
import alluxio.wire.TtlAction;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
public final class CreateDirectoryOptions extends alluxio.file.options.CreateDirectoryOptions {
  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
  }

  /**
   * Constructs an instance of {@link CreateDirectoryOptions} from {@link CreateDirectoryTOptions}.
   * The option of permission is constructed with the username obtained from thrift
   * transport.
   *
   * @param options the {@link CreateDirectoryTOptions} to use
   */
//  public CreateDirectoryOptions(CreateDirectoryTOptions options) {
//    this();
//    if (options != null) {
//      if (options.isSetCommonOptions()) {
//        mCommonOptions = new CommonOptions(options.getCommonOptions());
//      }
//      mAllowExists = options.isAllowExists();
//      mPersisted = options.isPersisted();
//      mRecursive = options.isRecursive();
//      mTtl = options.getTtl();
//      mTtlAction = ThriftUtils.fromThrift(options.getTtlAction());
//      if (SecurityUtils.isAuthenticationEnabled()) {
//        mOwner = SecurityUtils.getOwnerFromThriftClient();
//        mGroup = SecurityUtils.getGroupFromThriftClient();
//      }
//      if (options.isSetMode()) {
//        mMode = new Mode(options.getMode());
//      } else {
//        mMode.applyDirectoryUMask();
//      }
//    }
//  }

  private CreateDirectoryOptions() {
    super();
    mAllowExists = false;
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
    mMode = ModeUtils.applyDirectoryUMask(mMode);
  }
}
