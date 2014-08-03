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

public class StorageId {

  public static int getStorageDirIndex(long storageId) {
    return (int) storageId & 0x00ff;
  }

  /**
   * generate storage id
   * 
   * @param level
   *          storage level of the storage dir
   * @param aliasValue
   *          storage level alias value of the storage dir
   * @param dirIndex
   *          index of the storage dir
   * @return storage id
   */
  public static long getStorageId(int level, int aliasValue, int dirIndex) {
    return (level << 24) + (aliasValue << 16) + dirIndex;
  }

  /**
   * get storage level alias value from the storage id
   * 
   * @param storageId
   * @return value of storage level alias
   */
  public static int getStorageLevelAliasValue(long storageId) {
    return ((int) storageId >> 16) & 0x0f;
  }

  /**
   * get storage tier index from the storage id
   * 
   * @param storageId
   * @return index of the storage tier
   */
  public static int getStorageTierIndex(long storageId) {
    return ((int) storageId >> 24) & 0x0f;
  }
}