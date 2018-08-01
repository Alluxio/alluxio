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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for checking the consistency of a path.
 */
@NotThreadSafe
public final class CheckConsistencyOptions extends alluxio.file.options.CheckConsistencyOptions {
  /**
   * @return the default {@link CheckConsistencyOptions}
   */
  public static CheckConsistencyOptions defaults() {
    return new CheckConsistencyOptions();
  }

  private CheckConsistencyOptions() {
    mCommonOptions = CommonOptions.defaults();
  }

  /**
   * Constructs an instance of {@link CheckConsistencyOptions} from
   * {@link alluxio.thrift.CheckConsistencyTOptions}.
   *
   * @param options the {@link alluxio.thrift.CheckConsistencyTOptions} to use
   */
//  public CheckConsistencyOptions(CheckConsistencyTOptions options) {
//    this();
//    if (options != null) {
//      if (options.isSetCommonOptions()) {
//        mCommonOptions = new CommonOptions(options.getCommonOptions());
//      }
//    }
//  }
}
