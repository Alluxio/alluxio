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

package alluxio.file.options;

import alluxio.underfs.UfsStatus;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 *
 * @param <T> the type of the concrete subclass
 */
@NotThreadSafe
public abstract class CreateDirectoryOptions<T extends CreateDirectoryOptions>
    extends CreatePathOptions<T> {
  protected boolean mAllowExists;
  protected UfsStatus mUfsStatus;

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the {@link UfsStatus}
   */
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
  }

  /**
   * @param allowExists the allowExists flag value to use; it specifies whether an exception
   *        should be thrown if the object being made already exists.
   * @return the updated options object
   */
  public T setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return getThis();
  }

  /**
   * @param ufsStatus the {@link UfsStatus}; It sets the optional ufsStatus as an optimization
   * @return the updated options object
   */
  public T setUfsStatus(UfsStatus ufsStatus) {
    mUfsStatus = ufsStatus;
    return getThis();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateDirectoryOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    CreateDirectoryOptions that = (CreateDirectoryOptions) o;
    return Objects.equal(mAllowExists, that.mAllowExists)
        && Objects.equal(mUfsStatus, that.mUfsStatus);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mAllowExists, mUfsStatus);
  }

  @Override
  public String toString() {
    return toStringHelper().add("allowExists", mAllowExists)
        .add("ufsStatus", mUfsStatus).toString();
  }
}
