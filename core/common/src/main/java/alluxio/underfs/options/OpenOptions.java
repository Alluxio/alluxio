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
 * Method options for file locations in {@link alluxio.underfs.UnderFileSystem}.
 */
@PublicApi
@NotThreadSafe
public final class OpenOptions {
  // Offset within file in bytes
  private long mOffset;

  private long mLength;

  private boolean mPositionShort;

  /**
   * If true, attempt to recover after failed opened attempts. Extra effort may be required in
   * order to recover from a failed open.
   */
  private boolean mRecoverFailedOpen;

  /**
   * @return the default {@link OpenOptions}
   */
  public static OpenOptions defaults() {
    return new OpenOptions();
  }

  /**
   * Constructs a default {@link OpenOptions}.
   */
  private OpenOptions() {
    mOffset = 0;
    mLength = Long.MAX_VALUE;
    mRecoverFailedOpen = false;
    mPositionShort = false;
  }

  /**
   * @return offset from the start of a file in bytes
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * @return the maximum length of a file
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return true if failed open attempts should be recovered
   */
  public boolean getRecoverFailedOpen() {
    return mRecoverFailedOpen;
  }

  /**
   * @return true, if the operation is using positioned read to a small buffer size
   */
  public boolean getPositionShort() {
    return mPositionShort;
  }

  /**
   * Sets the offset from the start of a file to be opened for reading.
   *
   * @param offset within a file in bytes
   * @return the updated option object
   */
  public OpenOptions setOffset(long offset) {
    mOffset = offset;
    return this;
  }

  /**
   * @param length the maximum length of the file
   * @return the updated option object
   */
  public OpenOptions setLength(long length) {
    mLength = length;
    return this;
  }

  /**
   * @param recover true if failed open attempts should be recovered
   * @return the updated option object
   */
  public OpenOptions setRecoverFailedOpen(boolean recover) {
    mRecoverFailedOpen = recover;
    return this;
  }

  /**
   * @param positionShort whether the operation is positioned read to a small buffer
   * @return the updated option object
   */
  public OpenOptions setPositionShort(boolean positionShort) {
    mPositionShort = positionShort;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpenOptions)) {
      return false;
    }
    OpenOptions that = (OpenOptions) o;
    return Objects.equal(mOffset, that.mOffset)
        && Objects.equal(mLength, that.mLength)
        && Objects.equal(mRecoverFailedOpen, that.mRecoverFailedOpen)
        && Objects.equal(mPositionShort, that.mPositionShort);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOffset, mLength, mRecoverFailedOpen, mPositionShort);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("offset", mOffset)
        .add("length", mLength)
        .add("recoverFailedOpen", mRecoverFailedOpen)
        .add("positionShort", mPositionShort)
        .toString();
  }
}
