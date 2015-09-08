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

package tachyon.client;

/**
 * Specifies the type of data interaction with Tachyon's Under Storage. This is not applicable
 * for reads. Using the NO_PERSIST type will mean data is only written to Tachyon storage and may
 * be permanently lost when evicted. Only temporary data is suggested to be stored this way.
 */
public enum UnderStorageType {
  /** Write to Under Storage synchronously */
  PERSIST(1),

  /** Do not write to Under Storage */
  NO_PERSIST(2);

  private final int mValue;
  UnderStorageType(int value) {
    mValue = value;
  }

  public boolean isPersist() {
    return mValue == PERSIST.mValue;
  }
}
