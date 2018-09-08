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

package alluxio.file.options;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 *
 * @param <T> the type of the concrete subclass
 */
@NotThreadSafe
public abstract class CreateFileOptions<T extends CreateFileOptions<T>>
    extends CreatePathOptions<T> {
  protected long mBlockSizeBytes;
  protected boolean mCacheable;

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public T setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return getThis();
  }

  /**
   * @return true if file is cacheable
   */
  public boolean isCacheable() {
    return mCacheable;
  }

  /**
   * @param cacheable true if the file is cacheable, false otherwise
   * @return the updated options object
   */
  public T setCacheable(boolean cacheable) {
    mCacheable = cacheable;
    return getThis();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateFileOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    CreateFileOptions that = (CreateFileOptions) o;
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes)
        && Objects.equal(mCacheable, that.mCacheable);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mBlockSizeBytes, mCacheable);
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("cacheable", mCacheable)
        .toString();
  }
}
