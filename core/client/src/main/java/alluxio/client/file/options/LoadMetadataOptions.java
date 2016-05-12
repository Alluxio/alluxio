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
import alluxio.thrift.LoadMetadataTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for loading the metadata.
 */
@PublicApi
@NotThreadSafe
public final class LoadMetadataOptions {
  private boolean mRecursive;
  private boolean mLoadDirectChildren;

  /**
   * @return the default {@link LoadMetadataOptions}
   */
  public static LoadMetadataOptions defaults() {
    return new LoadMetadataOptions();
  }

  private LoadMetadataOptions() {
    mRecursive = false;
    mLoadDirectChildren = false;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the load direct children flag. It specifies whether the direct children should
   * be loaded.
   */
  public boolean isLoadDirectChildren() {
    return mLoadDirectChildren;
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

  /**
   * Sets the load direct children flag.
   *
   * @param loadDirectChildren the load direct children flag. It specifies whether the direct
   *                           children should be loaded.
   * @return the updated object
   */
  public LoadMetadataOptions setLoadDirectChildren(boolean loadDirectChildren) {
    mLoadDirectChildren = loadDirectChildren;
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
    return Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mLoadDirectChildren, that.mLoadDirectChildren);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecursive, mLoadDirectChildren);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("recursive", mRecursive)
        .add("loadDirectChildren", mLoadDirectChildren).toString();
  }

  /**
   * @return the thrift representation of the options
   */
  public LoadMetadataTOptions toThrift() {
    LoadMetadataTOptions options = new LoadMetadataTOptions();
    options.setRecursive(mRecursive);
    options.setLoadDirectChildren(mLoadDirectChildren);
    return options;
  }
}
