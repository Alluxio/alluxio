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

package alluxio.worker.block.meta;

import java.util.List;

import javax.annotation.Nullable;

/**
 * Represents a tier of storage, for example memory or SSD. It serves as a container of
 * {@link StorageDir} which actually contains metadata information about blocks stored and space
 * used/available.
 */
public interface StorageTier {
  /**
   * @return the tier ordinal
   */
  int getTierOrdinal();

  /**
   * @return the tier alias
   */
  String getTierAlias();

  /**
   * @return the capacity (in bytes)
   */
  long getCapacityBytes();

  /**
   * @return the remaining capacity (in bytes)
   */
  long getAvailableBytes();

  /**
   * Returns a directory for the given index.
   *
   * @param dirIndex the directory index
   * @return a directory, or null if the directory does not exist
   */
  @Nullable
  StorageDir getDir(int dirIndex);

  /**
   * @return a list of directories in this tier
   */
  List<StorageDir> getStorageDirs();

  /**
   * @return a list of lost storage paths
   */
  List<String> getLostStorage();

  /**
   * Removes a directory.
   * @param dir directory to be removed
   */
  void removeStorageDir(StorageDir dir);
}
