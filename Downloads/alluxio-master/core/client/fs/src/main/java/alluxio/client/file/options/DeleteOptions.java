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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.thrift.DeleteTOptions;
import alluxio.wire.CommonOptions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for deleting a file.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class DeleteOptions {
  private CommonOptions mCommonOptions;
  private boolean mRecursive;
  private boolean mAlluxioOnly;
  private boolean mUnchecked;

  /**
   * @return the default {@link DeleteOptions}
   */
  public static DeleteOptions defaults() {
    return new DeleteOptions();
  }

  private DeleteOptions() {
    mCommonOptions = CommonOptions.defaults();
    mRecursive = false;
    mAlluxioOnly = false;
    mUnchecked =
        Configuration.getBoolean(PropertyKey.USER_FILE_DELETE_UNCHECKED);
  }

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }
  /**
   * @return the recursive flag value; if the object to be deleted is a directory, the flag
   *         specifies whether the directory content should be recursively deleted as well
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return return the value of the flag that indicates whether the file should be
   *         deleted in Alluxio only, or in UFS as well
   */
  public boolean isAlluxioOnly() {
    return mAlluxioOnly;
  }

  /**
   * @return if the UFS sync check should be skipped
   */
  public boolean isUnchecked() {
    return mUnchecked;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public DeleteOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
  }

  /**
   * @param recursive the recursive flag value to use; if the object to be deleted is a directory,
   *        the flag specifies whether the directory content should be recursively deleted as well
   * @return the updated options object
   */
  public DeleteOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param alluxioOnly the value to use for the flag that indicates whether the file should be
   *        deleted in Alluxio only, or in UFS as well
   * @return the updated options object
   */
  public DeleteOptions setAlluxioOnly(boolean alluxioOnly) {
    mAlluxioOnly = alluxioOnly;
    return this;
  }

  /**
   * @param unchecked whether to skip UFS sync check
   * @return the updated options object
   */
  public DeleteOptions setUnchecked(boolean unchecked) {
    mUnchecked = unchecked;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeleteOptions)) {
      return false;
    }
    DeleteOptions that = (DeleteOptions) o;
    return Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mAlluxioOnly, that.mAlluxioOnly)
        && Objects.equal(mUnchecked, that.mUnchecked);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecursive, mAlluxioOnly, mUnchecked, mCommonOptions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("recursive", mRecursive)
        .add("alluxioOnly", mAlluxioOnly)
        .add("unchecked", mUnchecked)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public DeleteTOptions toThrift() {
    DeleteTOptions options = new DeleteTOptions();
    options.setRecursive(mRecursive);
    options.setAlluxioOnly(mAlluxioOnly);
    options.setUnchecked(mUnchecked);
    options.setCommonOptions(mCommonOptions.toThrift());
    return options;
  }
}
