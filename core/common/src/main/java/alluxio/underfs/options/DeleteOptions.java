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
 * Method options for deleting a directory in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public final class DeleteOptions {
  // Whether to delete a directory with children
  private boolean mRecursive;

  /**
   * @return the default {@link DeleteOptions}
   */
  public static DeleteOptions defaults() {
    return new DeleteOptions();
  }

  /**
   * Constructs a default {@link DeleteOptions}.
   */
  private DeleteOptions() {
    mRecursive = false;
  }

  /**
   * @return whether to delete a non-empty directory
   */
  public boolean isRecursive() {
    return mRecursive;
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
