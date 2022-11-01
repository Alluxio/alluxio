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

import java.util.Optional;

/**
 * Options for creating the {@link alluxio.client.file.ufs.UfsBaseFileSystem}.
 */
public class UfsFileSystemOptions {
  private Optional<String> mUfsAddress;

  /**
   * Creates a new instance of {@link UfsFileSystemOptions}.
   *
   * @param ufsAddress the ufs address
   */
  private UfsFileSystemOptions(Optional<String> ufsAddress) {
    mUfsAddress = Preconditions.checkNotNull(ufsAddress);
  }

  /**
   * @return the ufs address
   */
  public Optional<String> getUfsAddress() {
    return mUfsAddress;
  }

  /**
   * Builder class for {@link UfsFileSystemOptions}.
   */
  public static final class Builder {
    private String mUfsAddress;

    /**
     * Constructor.
     */
    public Builder() {}

    /**
     * @param ufsAddress the ufs address
     * @return the builder
     */
    public Builder setUfsAddress(String ufsAddress) {
      mUfsAddress = ufsAddress;
      return this;
    }

    /**
     * @return the worker net address
     */
    public UfsFileSystemOptions build() {
      return new UfsFileSystemOptions(mUfsAddress == null || mUfsAddress.isEmpty()
          ? Optional.empty() : Optional.of(mUfsAddress));
    }
  }
}

