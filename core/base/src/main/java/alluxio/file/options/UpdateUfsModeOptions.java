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

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Objects;

import alluxio.underfs.UfsMode;

/**
 * Method options for updating operation mode of a ufs path.
 */
@NotThreadSafe
public class UpdateUfsModeOptions {
  protected CommonOptions mCommonOptions;
  protected UfsMode mUfsMode;

  protected UpdateUfsModeOptions() {}

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return the Ufs mode
   */
  public UfsMode getUfsMode() {
    return mUfsMode;
  }

  /**
   * @param ufsMode the Ufs mode to set
   * @return the updated option object
   */
  public <T extends UpdateUfsModeOptions> T setUfsMode(UfsMode ufsMode) {
    mUfsMode = ufsMode;
    return (T) this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UpdateUfsModeOptions)) {
      return false;
    }
    UpdateUfsModeOptions that = (UpdateUfsModeOptions) o;
    return Objects.equal(mUfsMode, that.mUfsMode);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mUfsMode);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("ufsmode", mUfsMode)
        .toString();
  }
}
