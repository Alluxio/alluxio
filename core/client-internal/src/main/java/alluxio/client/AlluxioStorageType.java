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
 * Specifies the type of data interaction with Tachyon. This option is for developers and advanced
 * users. See {@link WriteType} and {@link ReadType}.
 * <ul>
 * <li>For a write operation, this determines whether the data will be written into Tachyon storage.
 * Metadata will always be updated in Tachyon space.</li>
 * <li>For a read operation, this determines whether fully read blocks will be stored in Tachyon
 * storage.</li>
 * </ul>
 */
@ThreadSafe
public enum AlluxioStorageType {
  /** Put the data reading or writing in Tachyon storage. */
  STORE(1),

  /** Do not put data to Tachyon. */
  NO_STORE(2),

  /**
   * Same as {@link #STORE} for writes. Will move the data to highest tier before access for reads.
   */
  PROMOTE(3);

  private final int mValue;

  AlluxioStorageType(int value) {
    mValue = value;
  }

  /**
   * @return whether the data should be put in Tachyon storage
   */
  public boolean isStore() {
    return mValue == STORE.mValue || mValue == PROMOTE.mValue;
  }

  /**
   * @return whether the data should be promoted to the highest tier
   */
  public boolean isPromote() {
    return mValue == PROMOTE.mValue;
  }
}
