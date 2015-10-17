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
 * Used to identify StorageDir in tiered store.
 */
public class StorageDirId {
  static final long UNKNOWN = -1;

  /**
   * Compare storage level of StorageDirs
   *
   * @param storageDirIdLeft The left value of StorageDirId to be compared
   * @param storageDirIdRight The right value of StorageDirId to be compared
   * @return negative if storage level of left StorageDirId is higher than the right one, zero if
   *         equals, positive if lower.
   */
  public static int compareStorageLevel(long storageDirIdLeft, long storageDirIdRight) {
    return getStorageLevelAliasValue(storageDirIdLeft)
        - getStorageLevelAliasValue(storageDirIdRight);
  }

  /**
   * Generate StorageDirId from given information
   *
   * @param level storage level of the StorageTier which contains the StorageDir
   * @param storageLevelAliasValue StorageLevelAlias value of the StorageTier
   * @param dirIndex index of the StorageDir
   * @return StorageDirId generated
   */
  public static long getStorageDirId(int level, int storageLevelAliasValue, int dirIndex) {
    return (level << 24) + (storageLevelAliasValue << 16) + dirIndex;
  }

  /**
   * Get index of the StorageDir in the StorageTier which contains it
   *
   * @param storageDirId Id of the StorageDir
   * @return index of the StorageDir
   */
  public static int getStorageDirIndex(long storageDirId) {
    return (int) storageDirId & 0x00ff;
  }

  /**
   * Get storage level of StorageTier which contains the StorageDir
   *
   * @param storageDirId Id of the StorageDir
   * @return storage level of the StorageTier which contains the StorageDir
   */
  public static int getStorageLevel(long storageDirId) {
    return ((int) storageDirId >> 24) & 0x0f;
  }

  /**
   * Get StorageLevelAlias from StorageDirId
   *
   * @param storageDirId Id of the StorageDir
   * @return the StorageLevelAlias
   */
  public static StorageLevelAlias getStorageLevelAlias(long storageDirId) {
    return StorageLevelAlias.values()[getStorageLevelAliasValue(storageDirId) - 1];
  }

  /**
   * Get StorageLevelAlias value from StorageDirId
   *
   * @param storageDirId Id of the StorageDir
   * @return value of StorageLevelAlias
   */
  public static int getStorageLevelAliasValue(long storageDirId) {
    return ((int) storageDirId >> 16) & 0x0f;
  }

  /**
   * Check whether the value of StorageDirId is UNKNOWN
   *
   * @param storageDirId Id of the StorageDir
   * @return true if StorageDirId is UNKNOWN, false otherwise
   */
  public static boolean isUnknown(long storageDirId) {
    return storageDirId == UNKNOWN;
  }

  /**
   * Get unknown value of StorageDirId, which indicates the StorageDir is unknown
   *
   * @return UNKNOWN value of StorageDirId
   */
  public static long unknownId() {
    return UNKNOWN;
  }

  private StorageDirId() {}
}
