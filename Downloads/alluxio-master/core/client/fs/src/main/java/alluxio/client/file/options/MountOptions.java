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
import alluxio.thrift.MountTOptions;
import alluxio.wire.CommonOptions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for mounting a path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class MountOptions {
  private CommonOptions mCommonOptions;
  private boolean mReadOnly;
  private Map<String, String> mProperties;
  private boolean mShared;

  /**
   * @return the default {@link MountOptions}
   */
  public static MountOptions defaults() {
    return new MountOptions();
  }

  /**
   * Creates a new instance with default values.
   */
  private MountOptions() {
    mCommonOptions = CommonOptions.defaults();
    mReadOnly = false;
    mProperties = new HashMap<>();
    mShared = false;
  }

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return the value of the readonly flag; if true, no write or create operations are allowed
   *         under the mount point.
   */
  public boolean isReadOnly() {
    return mReadOnly;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public MountOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
  }

  /**
   * Sets the readonly flag.
   *
   * @param readOnly the readonly flag value to use; if true, no write or create operations are
   *        allowed under the mount point.
   * @return the updated options object
   */
  public MountOptions setReadOnly(boolean readOnly) {
    mReadOnly = readOnly;
    return this;
  }

  /**
   * @return the properties map
   */
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(mProperties);
  }

  /**
   * @param properties the properties map to use
   * @return the updated options object
   */
  public MountOptions setProperties(Map<String, String> properties) {
    mProperties = properties;
    return this;
  }

  /**
   * @return the value of the shared flag; if true, the mounted point is shared with all Alluxio
   *         users.
   */
  public boolean isShared() {
    return mShared;
  }

  /**
   * @param shared the shared flag value to set; if true, the mounted point is shared with all
   *               Alluxio users.
   * @return the updated option object
   */
  public MountOptions setShared(boolean shared) {
    mShared = shared;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MountOptions)) {
      return false;
    }
    MountOptions that = (MountOptions) o;
    return Objects.equal(mReadOnly, that.mReadOnly)
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mProperties, that.mProperties)
        && Objects.equal(mShared, that.mShared);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mReadOnly, mProperties, mShared, mCommonOptions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("readonly", mReadOnly)
        .add("properties", mProperties)
        .add("shared", mShared)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public MountTOptions toThrift() {
    MountTOptions options = new MountTOptions();
    options.setReadOnly(mReadOnly);
    if (mProperties != null && !mProperties.isEmpty()) {
      options.setProperties(mProperties);
    }
    options.setShared(mShared);
    options.setCommonOptions(mCommonOptions.toThrift());
    return options;
  }
}
