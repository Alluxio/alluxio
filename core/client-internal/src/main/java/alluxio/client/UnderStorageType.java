/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Specifies the type of data interaction with Alluxio's Under Storage. This is not applicable for
 * reads. Only writing temporary data is suggested to use type {@link #NO_PERSIST} where writing to
 * Under Storage will be skipped and data may be lost when evicted from Alluxio storage.
 *
 * This option is for developers and advanced users. See {@link WriteType} and {@link ReadType}.
 */
@ThreadSafe
public enum UnderStorageType {
  /** Persist data to Under Storage synchronously. */
  SYNC_PERSIST(1),

  /** Do not persist data to Under Storage. */
  NO_PERSIST(2),

  /** Persist data to Under Storage asynchronously. */
  ASYNC_PERSIST(3);

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
