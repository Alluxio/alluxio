/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for mounting a path.
 */
@PublicApi
@NotThreadSafe
public final class MountOptions {
  private boolean mReadOnly;

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
    mReadOnly = false;
  }

  /**
   * @return the value of the readonly flag; if true, no write or create operations are allowed
   *         under the mount point.
   */
  public boolean isReadOnly() {
    return mReadOnly;
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
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("readonly", mReadOnly).toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public MountTOptions toThrift() {
    MountTOptions options = new MountTOptions();
    options.setReadOnly(mReadOnly);
    return options;
  }
}
