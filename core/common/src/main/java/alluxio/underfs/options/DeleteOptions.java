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

package alluxio.underfs.options;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Objects;

import alluxio.annotation.PublicApi;

/**
 * Method options for deleting a directory in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public final class DeleteOptions {
  // Whether to not delete the directory itself
  private boolean mChildrenOnly;
  // Whether to delete a directory recursively
  private boolean mRecursive;

  /**
   * Constructs a default {@link DeleteOptions}.
   */
  public DeleteOptions() {
    mChildrenOnly = false;
    mRecursive = false;
  }

  /**
   * @return whether to delete children only
   */
  public boolean getChildrenOnly() {
    return mChildrenOnly;
  }

  /**
   * @return recursive
   */
  public boolean getRecursive() {
    return mRecursive;
  }

  /**
   * Sets whether to delete children only.
   *
   * @param  whether to delete children only
   * @return the updated option object
   */
  public DeleteOptions setChildrenOnly(boolean childrenOnly) {
    mChildrenOnly = childrenOnly;
    return this;
  }

  /**
   * Sets recursive delete.
   *
   * @param recursive whether to delete recursively
   * @return the updated option object
   */
  public DeleteOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
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
    return Objects.equal(mChildrenOnly, that.mChildrenOnly)
        && Objects.equal(mRecursive, that.mRecursive);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mChildrenOnly, mRecursive);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("children_only", mChildrenOnly)
        .add("recursive", mRecursive)
        .toString();
  }
}
