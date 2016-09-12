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

package alluxio.client;

import alluxio.annotation.PublicApi;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Specifies the type of data interaction with Alluxio's Under Storage. This is not applicable for
 * reads. Only writing temporary data is suggested to use type {@link #NO_PERSIST} where writing to
 * Under Storage will be skipped and data may be lost when evicted from Alluxio storage.
 *
 * This option is for developers and advanced users. See {@link WriteType} and {@link ReadType}.
 */
@PublicApi
@ThreadSafe
public enum UnderStorageType {
  /** Persist data to Under Storage synchronously. */
  SYNC_PERSIST(1),

  /** Do not persist data to Under Storage. */
  NO_PERSIST(2),

  /** Persist data to Under Storage asynchronously. */
  ASYNC_PERSIST(3),
  ;

  private final int mValue;

  UnderStorageType(int value) {
    mValue = value;
  }

  /**
   * @return whether the data should be persisted to Under Storage synchronously
   */
  public boolean isSyncPersist() {
    return mValue == SYNC_PERSIST.mValue;
  }

  /**
   * @return whether the data should be persisted to Under Storage asynchronously
   */
  public boolean isAsyncPersist() {
    return mValue == ASYNC_PERSIST.mValue;
  }
}
