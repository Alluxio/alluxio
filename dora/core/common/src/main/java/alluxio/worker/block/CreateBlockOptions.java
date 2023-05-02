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

package alluxio.worker.block;

import alluxio.annotation.PublicApi;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for create a block.
 */
@PublicApi
@NotThreadSafe
public final class CreateBlockOptions {
  private String mAlluxioPath;
  private long mInitialBytes;
  private String mMedium;

  /**
   * Constructor.
   *
   * @param alluxioPath the alluxio path
   * @param medium the medium
   * @param initialBytes the initialBytes
   */
  public CreateBlockOptions(@Nullable String alluxioPath, @Nullable String medium,
      long initialBytes) {
    mAlluxioPath = alluxioPath;
    mMedium = medium;
    mInitialBytes = initialBytes;
  }

  /**
   * @return the alluxio path
   */
  @Nullable
  public String getAlluxioPath() {
    return mAlluxioPath;
  }

  /**
   * @return the initial bytes
   */
  public long getInitialBytes() {
    return mInitialBytes;
  }

  /**
   * @return the medium
   */
  @Nullable
  public String getMedium() {
    return mMedium;
  }

  /**
   * @param alluxioPath the alluxio path
   * @return the CreateBlockOptions
   */
  public CreateBlockOptions setAlluxioPath(String alluxioPath) {
    mAlluxioPath = alluxioPath;
    return this;
  }

  /**
   * @param initialBytes the intial bytes
   * @return the CreateBlockOptions
   */
  public CreateBlockOptions setInitialBytes(long initialBytes) {
    mInitialBytes = initialBytes;
    return this;
  }

  /**
   * @param medium the medium
   * @return the CreateBlockOptions
   */
  public CreateBlockOptions setMedium(String medium) {
    mMedium = medium;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateBlockOptions)) {
      return false;
    }
    CreateBlockOptions that = (CreateBlockOptions) o;
    return Objects.equal(mAlluxioPath, that.getAlluxioPath())
        && Objects.equal(mInitialBytes, that.mInitialBytes)
        && Objects.equal(mMedium, that.getMedium());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        mAlluxioPath,
        mInitialBytes,
        mMedium
    );
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("alluxioPath", mAlluxioPath)
        .add("initialBytes", mInitialBytes)
        .add("medium", mMedium)
        .toString();
  }
}

