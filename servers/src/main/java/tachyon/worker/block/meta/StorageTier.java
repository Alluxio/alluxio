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

package tachyon.worker.block.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.OutOfSpaceException;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerContext;

/**
 * Represents a tier of storage, for example memory or SSD. It serves as a container of
 * {@link StorageDir} which actually contains metadata information about blocks stored and space
 * used/available.
 * <p>
 * This class does not guarantee thread safety.
 */
public final class StorageTier {
  /** Alias of this tier, e.g., memory tier is 1, SSD tier is 2 and HDD tier is 3 */
  private final int mTierAlias;
  /** Level of this tier in tiered storage, highest level is 0 */
  private final int mTierLevel;
  /** Total capacity of all StorageDirs in bytes */
  private long mCapacityBytes;
  private List<StorageDir> mDirs;

  private StorageTier(int tierLevel) {
    mTierLevel = tierLevel;

    String tierLevelAliasProp =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, tierLevel);
    StorageLevelAlias alias = WorkerContext.getConf()
        .getEnum(tierLevelAliasProp, StorageLevelAlias.MEM);
    mTierAlias = alias.getValue();
  }

  private void initStorageTier() throws AlreadyExistsException, IOException,
      OutOfSpaceException {
    String workerDataFolder =
        WorkerContext.getConf().get(Constants.WORKER_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER);
    String tierDirPathConf =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, mTierLevel);
    String[] dirPaths = WorkerContext.getConf().get(tierDirPathConf, "/mnt/ramdisk").split(",");

    // Add the worker data folder path after each storage directory, the final path will be like
    // /mnt/ramdisk/tachyonworker
    for (int i = 0; i < dirPaths.length; i ++) {
      dirPaths[i] = PathUtils.concatPath(dirPaths[i].trim(), workerDataFolder);
    }

    String tierDirCapacityConf =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, mTierLevel);
    String[] dirQuotas = WorkerContext.getConf().get(tierDirCapacityConf, "0").split(",");

    mDirs = new ArrayList<StorageDir>(dirPaths.length);

    long totalCapacity = 0;
    for (int i = 0; i < dirPaths.length; i ++) {
      int index = i >= dirQuotas.length ? dirQuotas.length - 1 : i;
      long capacity = FormatUtils.parseSpaceSize(dirQuotas[index]);
      totalCapacity += capacity;
      mDirs.add(StorageDir.newStorageDir(this, i, capacity, dirPaths[i]));
    }
    mCapacityBytes = totalCapacity;
  }

  /**
   *
   * @param tierLevel the tier level
   * @return a new storage tier
   * @throws AlreadyExistsException if the tier already exists
   * @throws IOException if an I/O error occurred
   * @throws OutOfSpaceException if there is not enough space available
   */
  public static StorageTier newStorageTier(int tierLevel)
      throws AlreadyExistsException, IOException, OutOfSpaceException {
    StorageTier ret = new StorageTier(tierLevel);
    ret.initStorageTier();
    return ret;
  }

  /**
   * @return the tier alias
   */
  public int getTierAlias() {
    return mTierAlias;
  }

  /**
   * @return the tier level
   */
  public int getTierLevel() {
    return mTierLevel;
  }

  /**
   * @return the capacity (in bytes)
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the remaining capacity (in bytes)
   */
  public long getAvailableBytes() {
    long availableBytes = 0;
    for (StorageDir dir : mDirs) {
      availableBytes += dir.getAvailableBytes();
    }
    return availableBytes;
  }

  /**
   * Returns a directory for the given index.
   *
   * @param dirIndex the directory index
   * @return a directory
   */
  public StorageDir getDir(int dirIndex) {
    return mDirs.get(dirIndex);
  }

  /**
   * @return a list of directories in this tier
   */
  public List<StorageDir> getStorageDirs() {
    return Collections.unmodifiableList(mDirs);
  }
}
