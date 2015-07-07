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

package tachyon;

/**
 * Different storage level alias for StorageTier.
 */
public enum StorageLevelAlias {
  /**
   * Memory Layer
   */
  MEM(1),
  /**
   * SSD Layer
   */
  SSD(2),
  /**
   * HDD Layer
   */
  HDD(3);

  private int mValue;

  private StorageLevelAlias(int value) {
    mValue = value;
  }

  /**
   * Get value of the storage level alias
   * 
   * @return value of the storage level alias
   */
  public int getValue() {
    return mValue;
  }

  public static final int SIZE = StorageLevelAlias.values().length;
}
