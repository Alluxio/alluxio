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

import alluxio.annotation.PublicApi;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for listing a directory in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public final class ListOptions {
  // Whether to list a directory and all its sub-directories
  private boolean mRecursive;

  /**
   * @return the default {@link ListOptions}
   */
  public static ListOptions defaults() {
    return new ListOptions();
  }

  /**
   * Constructs a default {@link ListOptions}.
   */
  private ListOptions() {
    mRecursive = false;
  }

  /**
   * @return whether to list a directory recursively
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * Sets recursive list.
   *
   * @param recursive whether to list recursively
   * @return the updated option object
   */
  public ListOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ListOptions)) {
      return false;
    }
    ListOptions that = (ListOptions) o;
    return Objects.equal(mRecursive, that.mRecursive);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecursive);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("recursive", mRecursive)
        .toString();
  }
}
