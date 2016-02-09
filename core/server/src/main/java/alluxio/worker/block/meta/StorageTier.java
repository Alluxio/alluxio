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

package alluxio.worker.block.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import alluxio.Constants;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.WorkerContext;

/**
 * Represents a tier of storage, for example memory or SSD. It serves as a container of
 * {@link StorageDir} which actually contains metadata information about blocks stored and space
 * used/available.
 */
@NotThreadSafe
public final class StorageTier {
  /** Alias value of this tier in tiered storage. */
  private final String mTierAlias;
  /** Ordinal value of this tier in tiered storage, highest level is 0. */
  private final int mTierOrdinal;
  /** Total capacity of all StorageDirs in bytes. */
  private long mCapacityBytes;
  private List<StorageDir> mDirs;

  private StorageTier(String tierAlias) {
    mTierAlias = tierAlias;
    mTierOrdinal = new WorkerStorageTierAssoc(WorkerContext.getConf()).getOrdinal(tierAlias);
  }

  private void initStorageTier()
      throws BlockAlreadyExistsException, IOException, WorkerOutOfSpaceException {
    String workerDataFolder = WorkerContext.getConf().get(Constants.WORKER_DATA_FOLDER);
    String tierDirPathConf =
        String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, mTierOrdinal);
    String[] dirPaths = WorkerContext.getConf().get(tierDirPathConf).split(",");

    // Add the worker data folder path after each storage directory, the final path will be like
    // /mnt/ramdisk/alluxioworker
    for (int i = 0; i < dirPaths.length; i++) {
      dirPaths[i] = PathUtils.concatPath(dirPaths[i].trim(), workerDataFolder);
    }

    String tierDirCapacityConf =
        String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, mTierOrdinal);
    String[] dirQuotas = WorkerContext.getConf().get(tierDirCapacityConf).split(",");

    mDirs = new ArrayList<StorageDir>(dirPaths.length);

    long totalCapacity = 0;
    for (int i = 0; i < dirPaths.length; i++) {
      int index = i >= dirQuotas.length ? dirQuotas.length - 1 : i;
      long capacity = FormatUtils.parseSpaceSize(dirQuotas[index]);
      totalCapacity += capacity;
      mDirs.add(StorageDir.newStorageDir(this, i, capacity, dirPaths[i]));
    }
    mCapacityBytes = totalCapacity;
  }

  /**
   * Factory method to create {@link StorageTier}.
   *
   * @param tierAlias the tier alias
   * @return a new storage tier
   * @throws BlockAlreadyExistsException if the tier already exists
   * @throws IOException if an I/O error occurred
   * @throws WorkerOutOfSpaceException if there is not enough space available
   */
  public static StorageTier newStorageTier(String tierAlias)
      throws BlockAlreadyExistsException, IOException, WorkerOutOfSpaceException {
    StorageTier ret = new StorageTier(tierAlias);
    ret.initStorageTier();
    return ret;
  }

  /**
   * @return the tier ordinal
   */
  public int getTierOrdinal() {
    return mTierOrdinal;
  }

  /**
   * @return the tier alias
   */
  public String getTierAlias() {
    return mTierAlias;
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
