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
import alluxio.thrift.SetAclTOptions;
import alluxio.wire.CommonOptions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting ACL.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class SetAclOptions {
  private CommonOptions mCommonOptions;
  private boolean mRecursive;

  /**
   * @return the default {@link SetAclOptions}
   */
  public static SetAclOptions defaults() {
    return new SetAclOptions();
  }

  private SetAclOptions() {
    mCommonOptions = CommonOptions.defaults();
    mRecursive = false;
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
   * @param recursive the recursive setting to use
   * @return the updated options
   */
  public SetAclOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
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
        && Objects.equal(mRecursive, that.mRecursive);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCommonOptions, mRecursive);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("recursive", mRecursive)
        .toString();
  }

  /**
   * @return thrift representation of the options
   */
  public SetAclTOptions toThrift() {
    SetAclTOptions options = new SetAclTOptions();
    options.setCommonOptions(mCommonOptions.toThrift());
    options.setRecursive(mRecursive);
    return options;
  }
}
