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

package alluxio.master.file.options;

import alluxio.thrift.SetAclTOptions;
import alluxio.wire.CommonOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting ACL.
 */
@NotThreadSafe
public final class SetAclOptions {
  private CommonOptions mCommonOptions;
  private boolean mRecursive;
  private boolean mOpdefault;

  /**
   * @return the default {@link SetAclOptions}
   */
  public static SetAclOptions defaults() {
    return new SetAclOptions();
  }

  private SetAclOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
    mRecursive = false;
    mOpdefault = false;
  }

  /**
   * Create an instance of {@link SetAclOptions} from a {@link SetAclTOptions}.
   *
   * @param options the thrift representation of list status options
   */
  public SetAclOptions(SetAclTOptions options) {
    this();
    if (options != null) {
      if (options.isSetCommonOptions()) {
        mCommonOptions = new CommonOptions(options.getCommonOptions());
      }
      if (options.isSetRecursive()) {
        mRecursive = options.isRecursive();
      }
      if (options.isOpdefault()) {
        mOpdefault = options.isOpdefault();
      }
    }
  }

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return true if action should be performed recursively
   */
  public boolean getRecursive() {
    return mRecursive;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public SetAclOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
  }

  /**
   * Sets the {@link SetAclOptions#mRecursive}.
   *
   * @param recursive the recursive setting to use
   * @return the updated options
   */
  public SetAclOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  public SetAclOptions setDefault(boolean opDefault) {
    mOpdefault = opDefault;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SetAclOptions)) {
      return false;
    }
    SetAclOptions that = (SetAclOptions) o;
    return Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mOpdefault, that.mOpdefault);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCommonOptions, mRecursive, mOpdefault);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("recursive", mRecursive)
        .add ("default", mOpdefault)
        .toString();
  }
}
