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
import java.util.List;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * Represents a tier of storage, for example memory or SSD. It serves as a container of
 * {@link StorageDir} which actually contains metadata information about blocks stored and space
 * used/available.
 * <p>
 * This class does not guarantee thread safety.
 */
public class StorageTier {
  /** Alias of this tier, e.g., memory tier is 1, SSD tier is 2 and HDD tier is 3 */
  private final int mTierAlias;
  /** Level of this tier in tiered storage, highest level is 0 */
  private final int mTierLevel;
  /** Total capacity of all StorageDirs in bytes */
  private long mCapacityBytes;
  private List<StorageDir> mDirs;

  private StorageTier(TachyonConf tachyonConf, int tierLevel) {
    mTierLevel = tierLevel;

    String tierLevelAliasProp =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, tierLevel);
    StorageLevelAlias alias = tachyonConf.getEnum(tierLevelAliasProp, StorageLevelAlias.MEM);
    mTierAlias = alias.getValue();
  }

  private void initStorageTier(TachyonConf tachyonConf) throws IOException {
    String workerDataFolder =
        tachyonConf.get(Constants.WORKER_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER);
    String tierDirPathConf =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, mTierLevel);
    String[] dirPaths = tachyonConf.get(tierDirPathConf, "/mnt/ramdisk").split(",");

    // Add the worker data folder path after each storage directory, the final path will be like
    // /mnt/ramdisk/tachyonworker
    for (int i = 0; i < dirPaths.length; i ++) {
      dirPaths[i] = CommonUtils.concatPath(dirPaths[i].trim(), workerDataFolder);
    }

    String tierDirCapacityConf =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, mTierLevel);
    String[] dirQuotas = tachyonConf.get(tierDirCapacityConf, "0").split(",");

    mDirs = new ArrayList<StorageDir>(dirPaths.length);

    long totalCapacity = 0;
    for (int i = 0; i < dirPaths.length; i ++) {
      int index = i >= dirQuotas.length ? dirQuotas.length - 1 : i;
      long capacity = CommonUtils.parseSpaceSize(dirQuotas[index]);
      totalCapacity += capacity;
      mDirs.add(StorageDir.newStorageDir(this, i, capacity, dirPaths[i]));
    }
    mCapacityBytes = totalCapacity;
  }

  public static StorageTier newStorageTier(TachyonConf tachyonConf, int tierLevel)
      throws IOException {
    StorageTier ret = new StorageTier(tachyonConf, tierLevel);
    ret.initStorageTier(tachyonConf);
    return ret;
  }

  public int getTierAlias() {
    return mTierAlias;
  }

  public int getTierLevel() {
    return mTierLevel;
  }

  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  public long getAvailableBytes() {
    long availableBytes = 0;
    for (StorageDir dir : mDirs) {
      availableBytes += dir.getAvailableBytes();
    }
    return availableBytes;
  }

  public StorageDir getDir(int dirIndex) {
    return mDirs.get(dirIndex);
  }

  public List<StorageDir> getStorageDirs() {
    return mDirs;
  }
}
