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

import static java.util.Objects.requireNonNull;

import alluxio.annotation.PublicApi;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for create a block.
 */
@PublicApi
@NotThreadSafe
public final class CreateBlockOptions {
  private final long mInitialBytes;
  private final Optional<String> mMedium;

  /**
   * Constructor.
   *
   * @param medium the medium
   * @param initialBytes the initialBytes
   */
  public CreateBlockOptions(Optional<String> medium,
      long initialBytes) {
    mMedium = requireNonNull(medium, "medium is null");
    mInitialBytes = initialBytes;
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
  public Optional<String> getMedium() {
    return mMedium;
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
    return Objects.equal(mInitialBytes, that.mInitialBytes)
        && Objects.equal(mMedium, that.getMedium());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        mInitialBytes,
        mMedium
    );
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("initialBytes", mInitialBytes)
        .add("medium", mMedium)
        .toString();
  }
}

