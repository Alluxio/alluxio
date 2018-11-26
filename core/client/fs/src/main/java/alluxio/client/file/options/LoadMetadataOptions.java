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

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for loading the metadata.
 *
 * @deprecated since version 1.1 and will be removed in version 2.0
 */
@PublicApi
@NotThreadSafe
@Deprecated
public final class LoadMetadataOptions {
  private boolean mRecursive;

  /**
   * @return the default {@link LoadMetadataOptions}
   */
  public static LoadMetadataOptions defaults() {
    return new LoadMetadataOptions();
  }

  private LoadMetadataOptions() {
    mRecursive = false;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * Sets the recursive flag.
   *
   * @param recursive the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public LoadMetadataOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LoadMetadataOptions)) {
      return false;
    }
    LoadMetadataOptions that = (LoadMetadataOptions) o;
    return Objects.equal(mRecursive, that.mRecursive);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecursive);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("recursive", mRecursive)
        .toString();
  }
}
