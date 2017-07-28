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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.util.FormatUtils;
import alluxio.util.OSUtils;
import alluxio.util.ShellUtils;
import alluxio.util.UnixMountInfo;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents a tier of storage, for example memory or SSD. It serves as a container of
 * {@link StorageDir} which actually contains metadata information about blocks stored and space
 * used/available.
 */
@NotThreadSafe
public final class StorageTier {
  private static final Logger LOG = LoggerFactory.getLogger(StorageTier.class);

  /** Alias value of this tier in tiered storage. */
  private final String mTierAlias;
  /** Ordinal value of this tier in tiered storage, highest level is 0. */
  private final int mTierOrdinal;
  /** Total capacity of all StorageDirs in bytes. */
  private long mCapacityBytes;
  private List<StorageDir> mDirs;

  private StorageTier(String tierAlias) {
    mTierAlias = tierAlias;
    mTierOrdinal = new WorkerStorageTierAssoc().getOrdinal(tierAlias);
  }

  private void initStorageTier()
      throws BlockAlreadyExistsException, IOException, WorkerOutOfSpaceException {
    String workerDataFolder = Configuration.get(PropertyKey.WORKER_DATA_FOLDER);
    String tmpDir = Configuration.get(PropertyKey.WORKER_DATA_TMP_FOLDER);
    PropertyKey tierDirPathConf =
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(mTierOrdinal);
    String[] dirPaths = Configuration.get(tierDirPathConf).split(",");

    // Add the worker data folder path after each storage directory, the final path will be like
    // /mnt/ramdisk/alluxioworker
    for (int i = 0; i < dirPaths.length; i++) {
      dirPaths[i] = PathUtils.concatPath(dirPaths[i].trim(), workerDataFolder);
    }

    PropertyKey tierDirCapacityConf =
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(mTierOrdinal);
    String rawDirQuota = Configuration.get(tierDirCapacityConf);
    Preconditions.checkState(rawDirQuota.length() > 0, PreconditionMessage.ERR_TIER_QUOTA_BLANK);
    String[] dirQuotas = rawDirQuota.split(",");

    mDirs = new ArrayList<>(dirPaths.length);

    long totalCapacity = 0;
    for (int i = 0; i < dirPaths.length; i++) {
      int index = i >= dirQuotas.length ? dirQuotas.length - 1 : i;
      long capacity = FormatUtils.parseSpaceSize(dirQuotas[index]);
      totalCapacity += capacity;
      mDirs.add(StorageDir.newStorageDir(this, i, capacity, dirPaths[i]));

      // Delete tmp directory.
      String tmpDirPath = PathUtils.concatPath(dirPaths[i], tmpDir);
      try {
        FileUtils.deletePathRecursively(tmpDirPath);
      } catch (IOException e) {
        if (FileUtils.exists(tmpDirPath)) {
          LOG.error("Failed to clean up temporary directory: {}.", tmpDirPath);
        }
      }
    }
    mCapacityBytes = totalCapacity;
    if (mTierAlias.equals("MEM") && mDirs.size() == 1) {
      checkEnoughMemSpace(mDirs.get(0));
    }
  }

  /**
   * Checks that a tmpfs/ramfs backing the storage directory has enough capacity. If the storage
   * directory is not backed by tmpfs/ramfs or the size of the tmpfs/ramfs cannot be determined, a
   * warning is logged but no exception is thrown.
   *
   * @param storageDir the storage dir to check
   * @throws IllegalStateException if the tmpfs/ramfs is smaller than the configured memory size
   */
  private void checkEnoughMemSpace(StorageDir storageDir) {
    if (!OSUtils.isLinux()) {
      return;
    }
    List<UnixMountInfo> info;
    try {
      info = ShellUtils.getUnixMountInfo();
    } catch (IOException e) {
      LOG.warn("Failed to get mount information for verifying memory capacity: {}", e.getMessage());
      return;
    }
    boolean foundMountInfo = false;
    for (UnixMountInfo mountInfo : info) {
      Optional<String> mountPointOption = mountInfo.getMountPoint();
      Optional<String> fsTypeOption = mountInfo.getFsType();
      Optional<Long> sizeOption = mountInfo.getOptions().getSize();
      if (!mountPointOption.isPresent() || !fsTypeOption.isPresent() || !sizeOption.isPresent()) {
        continue;
      }
      String mountPoint = mountPointOption.get();
      String fsType = fsTypeOption.get();
      long size = sizeOption.get();
      try {
        // getDirPath gives something like "/mnt/tmpfs/alluxioworker".
        String rootStoragePath = PathUtils.getParent(storageDir.getDirPath());
        if (!PathUtils.cleanPath(mountPoint).equals(rootStoragePath)) {
          continue;
        }
      } catch (InvalidPathException e) {
        continue;
      }
      foundMountInfo = true;
      if ((fsType.equalsIgnoreCase("tmpfs") || fsType.equalsIgnoreCase("ramfs"))
          && size < storageDir.getCapacityBytes()) {
        throw new IllegalStateException(String.format(
            "%s is smaller than the configured size: %s size: %s, configured size: %s", fsType,
            fsType, FormatUtils.getSizeFromBytes(size),
            FormatUtils.getSizeFromBytes(storageDir.getCapacityBytes())));
      }
      break;
    }
    if (!foundMountInfo) {
      LOG.warn("Failed to verify memory capacity");
    }
  }

  /**
   * Factory method to create {@link StorageTier}.
   *
   * @param tierAlias the tier alias
   * @return a new storage tier
   * @throws BlockAlreadyExistsException if the tier already exists
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
