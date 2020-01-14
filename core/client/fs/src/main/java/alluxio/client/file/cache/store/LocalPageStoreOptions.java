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

package alluxio.client.file.cache.store;

import alluxio.Constants;

/**
 * Options used to instantiate the {@link LocalPageStore}.
 */
public class LocalPageStoreOptions extends PageStoreOptions {

  /**
   * The max number of buffers used to transfer data. Total memory usage will be equivalent to
   * {@link #mBufferPoolSize} * {@link #mBufferSize}
   */
  private int mBufferPoolSize;

  /**
   * The size of the buffers used to transfer data. It is recommended to set this at or near the
   * expected max page size.
   */
  private int mBufferSize;

  /**
   * Creates a new instance of {@link LocalPageStoreOptions}.
   */
  public LocalPageStoreOptions() {
    mBufferPoolSize = 32;
    mBufferSize = Constants.MB;
  }

  /**
   * @param bufferPoolSize sets the buffer pool size
   * @return the updated options
   */
  public LocalPageStoreOptions setBufferPoolSize(int bufferPoolSize) {
    mBufferPoolSize = bufferPoolSize;
    return this;
  }

  /**
   * @return the size of the buffer pool
   */
  public int getBufferPoolSize() {
    return mBufferPoolSize;
  }

  /**
   * @param bufferSize the number of buffers in the buffer pool
   * @return the updated options
   */
  public LocalPageStoreOptions setBufferSize(int bufferSize) {
    mBufferSize = bufferSize;
    return this;
  }

  /**
   * @return the number of items
   */
  public int getBufferSize() {
    return mBufferSize;
  }

  @Override
  public PageStoreType getType() {
    return PageStoreType.LOCAL;
  }
}
