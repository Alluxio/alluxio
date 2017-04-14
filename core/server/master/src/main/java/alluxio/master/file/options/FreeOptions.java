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

import alluxio.thrift.FreeTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for list status.
 */
@NotThreadSafe
public final class FreeOptions {
  private boolean mRecursive;
  private boolean mForced;

  /**
   * @return the default {@link FreeOptions}
   */
  public static FreeOptions defaults() {
    return new FreeOptions();
  }

  private FreeOptions() {
    mForced = false;
    mRecursive = false;
  }

  /**
   * Creates an instance of {@link FreeOptions} from a {@link FreeTOptions}.
   *
   * @param options the thrift representation of free options
   */
  public FreeOptions(FreeTOptions options) {
    mForced = options.isForced();
    mRecursive = options.isRecursive();
  }

  /**
   * @return the forced flag value; if the object to be freed is pinned, the flag specifies
   *         whether this object should still be freed
   */
  public boolean isForced() {
    return mForced;
  }

  /**
   * @return the recursive flag value; if the object to be freed is a directory, the flag specifies
   *         whether the directory content should be recursively freed as well
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * Sets the forced flag.
   *
   * @param forced the forced flag value; if the object to be freed is pinned, the flag specifies
   *         whether this object should still be freed
   * @return the updated options object
   */
  public FreeOptions setForced(boolean forced) {
    mForced = forced;
    return this;
  }

  /**
   * Sets the recursive flag.
   *
   * @param recursive the recursive flag value to use; if the object to be freed is a directory,
   *        the flag specifies whether the directory content should be recursively freed as well
   * @return the updated options object
   */
  public FreeOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FreeOptions)) {
      return false;
    }
    FreeOptions that = (FreeOptions) o;
    return Objects.equal(mForced, that.mForced) && Objects.equal(mRecursive, that.mRecursive);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mForced, mRecursive);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("forced", mForced)
        .add("recursive", mRecursive)
        .toString();
  }
}
