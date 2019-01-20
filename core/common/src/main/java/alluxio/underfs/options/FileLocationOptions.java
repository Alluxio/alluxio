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
public final class FileLocationOptions {
  // Offset within a file in bytes
  private long mOffset;

  /**
   * @return the default {@link FileLocationOptions}
   */
  public static FileLocationOptions defaults() {
    return new FileLocationOptions();
  }

  /**
   * Constructs a default {@link FileLocationOptions}.
   */
  private FileLocationOptions() {
    mOffset = 0;
  }

  /**
   * @return offset within a file in bytes
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * Sets the offset for which locations are to be queried.
   *
   * @param offset within a file in bytes
   * @return the updated option object
   */
  public FileLocationOptions setOffset(long offset) {
    mOffset = offset;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileLocationOptions)) {
      return false;
    }
    FileLocationOptions that = (FileLocationOptions) o;
    return Objects.equal(mOffset, that.mOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOffset);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("offset", mOffset)
        .toString();
  }
}
