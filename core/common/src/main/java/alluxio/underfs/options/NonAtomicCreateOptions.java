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

package alluxio.underfs.options;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options to complete creating a file in UnderFileSystem.
 */
@NotThreadSafe
public final class NonAtomicCreateOptions {

  private String mTemporaryPath;
  private String mPermanentPath;
  private CreateOptions mOptions;

  /**
   * Constructs a default {@link NonAtomicCreateOptions}.
   *
   * @param tempPath temporary path for write
   * @param permanentPath final path
   * @param options create options
   */
  public NonAtomicCreateOptions(String tempPath, String permanentPath, CreateOptions options) {
    mTemporaryPath = tempPath;
    mPermanentPath = permanentPath;
    mOptions = options;
  }

  /**
   * @return the temporary path
   */
  public String getTemporaryPath() {
    return mTemporaryPath;
  }

  /**
   * @return the final path
   */
  public String getPermanentPath() {
    return mPermanentPath;
  }

  /**
   * @return the create options
   */
  public CreateOptions getCreateOptions() {
    return mOptions;
  }
}
