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

import alluxio.annotation.PublicApi;
import alluxio.underfs.UnderFileSystem.UfsMode;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for updating operation mode of a path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class UpdateUfsModeOptions {
  private UfsMode mUfsMode;

  /**
   * @return the default {@link UpdateUfsModeOptions}
   */
  public static UpdateUfsModeOptions defaults() {
    return new UpdateUfsModeOptions();
  }

  private UpdateUfsModeOptions() {
    mUfsMode = UfsMode.READ_WRITE;
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
  public UpdateUfsModeOptions setUfsMode(UfsMode ufsMode) {
    mUfsMode = ufsMode;
    return this;
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
