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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Optional;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * Represents a tier of storage, for example memory or SSD. It serves as a container of
 * {@link StorageDir} which actually contains metadata information about blocks stored and space
 * used/available.
 * <p>
 * This class is not guarantee thread safety.
 */
public class StorageTier {
  private HashMap<Integer, StorageDir> mIdToStorageDirMap;
  private final int mTierId;

  public StorageTier(TachyonConf tachyonConf, int tier) {
    mTierId = tier;
    String tierDirPathConf =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, tier);
    String[] dirPaths = tachyonConf.get(tierDirPathConf, "/mnt/ramdisk").split(",");

    String tierDirCapacityConf =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, tier);
    String[] dirQuotas = tachyonConf.get(tierDirCapacityConf, "0").split(",");

    mIdToStorageDirMap = new HashMap<Integer, StorageDir>(dirPaths.length);

    for (int i = 0; i < dirPaths.length; i ++) {
      int index = i >= dirQuotas.length ? dirQuotas.length - 1 : i;
      long capacity = CommonUtils.parseSpaceSize(dirQuotas[index]);
      mIdToStorageDirMap.put(i, new StorageDir(i, capacity, dirPaths[i]));
    }
  }

  public int getTierId() {
    return mTierId;
  }

  public long getCapacityBytes() {
    long capacityBytes = 0;
    for (StorageDir dir : mIdToStorageDirMap.values()) {
      capacityBytes += dir.getCapacityBytes();
    }
    return capacityBytes;
  }

  public long getAvailableBytes() {
    long availableBytes = 0;
    for (StorageDir dir : mIdToStorageDirMap.values()) {
      availableBytes += dir.getAvailableBytes();
    }
    return availableBytes;
  }

  public Set<StorageDir> getStorageDirs() {
    return new HashSet<StorageDir>(mIdToStorageDirMap.values());
  }

  public Optional<BlockMeta> getBlockMeta(long blockId) {
    for (StorageDir dir : mIdToStorageDirMap.values()) {
      Optional<BlockMeta> optionalBlock = dir.getBlockMeta(blockId);
      if (optionalBlock.isPresent()) {
        return optionalBlock;
      }
    }
    return Optional.absent();
  }

  public Optional<BlockMeta> addBlockMeta(long userId, long blockId, long blockSize) {
    for (StorageDir dir : mIdToStorageDirMap.values()) {
      Optional<BlockMeta> optionalBlock = dir.addBlockMeta(userId, blockId, blockSize);
      if (optionalBlock.isPresent()) {
        return optionalBlock;
      }
    }
    return Optional.absent();
  }

  public boolean removeBlockMeta(long blockId) {
    for (StorageDir dir : mIdToStorageDirMap.values()) {
      if (dir.hasBlockMeta(blockId)) {
        return dir.removeBlockMeta(blockId);
      }
    }
    return false;
  }

}
