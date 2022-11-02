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

import com.google.common.base.Preconditions;

/**
 * Options for creating the {@link alluxio.client.file.ufs.UfsBaseFileSystem}.
 */
public class UfsFileSystemOptions {
  private final String mUfsAddress;

  /**
   * Creates a new instance of {@link UfsFileSystemOptions}.
   *
   * @param ufsAddress the ufs address
   */
  public UfsFileSystemOptions(String ufsAddress) {
    mUfsAddress = Preconditions.checkNotNull(ufsAddress);
  }

  /**
   * @return the ufs address
   */
  public String getUfsAddress() {
    return mUfsAddress;
  }
}
