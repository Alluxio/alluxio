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

import alluxio.thrift.DeleteTOptions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for deleting a file or a directory.
 */
@NotThreadSafe
public final class DeleteOptions extends alluxio.file.options.DeleteOptions {
  /**
   * @return the default {@link DeleteOptions}
   */
  public static DeleteOptions defaults() {
    return new DeleteOptions();
  }

  /**
   * @param options the {@link DeleteTOptions} to use
   */
//  public DeleteOptions(DeleteTOptions options) {
//    this();
//    if (options != null) {
//      if (options.isSetCommonOptions()) {
//        mCommonOptions = new CommonOptions(options.getCommonOptions());
//      }
//      mRecursive = options.isRecursive();
//      mAlluxioOnly = options.isAlluxioOnly();
//      mUnchecked = options.isUnchecked();
//    }
//  }

  private DeleteOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
    mRecursive = false;
    mAlluxioOnly = false;
    mUnchecked = false;
  }
}
