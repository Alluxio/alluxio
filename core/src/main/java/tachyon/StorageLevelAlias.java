/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.IOException;

public enum StorageLevelAlias {

  MEM(1), SSD(2), HDD(3);

  /**
   * Parse the storage level
   * 
   * @param storageLevel
   *          the String format of the storage level
   * @return the StorageLevel
   * @throws IOException
   */
  public static StorageLevelAlias getStorageLevel(String storageLevel) throws IOException {
    if (storageLevel.equals("MEM")) {
      return MEM;
    } else if (storageLevel.equals("SSD")) {
      return SSD;
    } else if (storageLevel.equals("HDD")) {
      return HDD;
    }

    throw new IOException("Unknown StorageLevel : " + storageLevel);
  }

  private int value;

  private StorageLevelAlias(int value) {
    this.value = value;
  }

  /**
   * get value of the storage level alias
   * 
   * @return value of the storage level alias
   */
  public int getValue() {
    return value;
  }
}