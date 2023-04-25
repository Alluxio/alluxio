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

package alluxio.testutils.underfs;

import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility functions for testing with {@link UnderFileSystem}.
 */
@ThreadSafe
public final class UnderFileSystemTestUtils {
  /**
   * Returns whether the given ufs address indicates a object storage ufs.
   *
   * @param ufsAddress the ufs address
   * @return true if the under file system is a object storage; false otherwise
   */
  public static boolean isObjectStorage(String ufsAddress) {
    return ufsAddress.startsWith(Constants.HEADER_S3)
        || ufsAddress.startsWith(Constants.HEADER_S3A)
        || ufsAddress.startsWith(Constants.HEADER_GCS)
        || ufsAddress.startsWith(Constants.HEADER_SWIFT)
        || ufsAddress.startsWith(Constants.HEADER_OSS);
  }

  private UnderFileSystemTestUtils() {} // prevent instantiation
}
