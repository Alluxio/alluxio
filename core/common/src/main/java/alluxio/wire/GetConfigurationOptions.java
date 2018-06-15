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

package alluxio.wire;

import alluxio.thrift.GetConfigurationTOptions;

import com.google.common.base.Objects;

/**
 * Options for getConfiguration operations from the Alluxio meta master.
 */
public class GetConfigurationOptions {
  private boolean mRawValue;

  /**
   * @return the default {@link GetConfigurationOptions}
   */
  public static GetConfigurationOptions defaults() {
    return new GetConfigurationOptions();
  }

  /**
   * Constructs an instance.
   */
  private GetConfigurationOptions() {
    mRawValue = true;
  }

  /**
   * @param tOpts thrift options
   * @return wire type options corresponding to the thrift options
   */
  public static GetConfigurationOptions fromThrift(GetConfigurationTOptions tOpts) {
    return new GetConfigurationOptions().setRawValue(tOpts.isRawValue());
  }

  /**
   * @return the thrift options corresponding to these options
   */
  public GetConfigurationTOptions toThrift() {
    return new GetConfigurationTOptions().setRawValue(mRawValue);
  }

  /**
   * @param rawValue whether to use raw value
   * @return the updated option instance
   */
  public GetConfigurationOptions setRawValue(boolean rawValue) {
    mRawValue = rawValue;
    return this;
  }

  /**
   * @return whether to use raw value
   */
  public boolean isRawValue() {
    return mRawValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetConfigurationOptions)) {
      return false;
    }
    GetConfigurationOptions that = (GetConfigurationOptions) o;
    return Objects.equal(mRawValue, that.mRawValue);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRawValue);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("rawValue", mRawValue)
        .toString();
  }
}
