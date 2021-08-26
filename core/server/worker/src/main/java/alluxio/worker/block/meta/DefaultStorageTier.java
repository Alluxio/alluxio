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

import alluxio.Constants;
import alluxio.WorkerStorageTierAssoc;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.util.CommonUtils;
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
import java.util.HashMap;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents a tier of storage, for example memory or SSD. It serves as a container of
 * {@link StorageDir} which actually contains metadata information about blocks stored and space
 * used/available.
 */
@NotThreadSafe
public final class DefaultStorageTier implements StorageTier {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageTier.class);

  /** Alias value of this tier in tiered storage. */
  private final String mTierAlias;
  /** Ordinal value of this tier in tiered storage, highest level is 0. */
  private final int mTierOrdinal;
  /** Total capacity of all StorageDirs in bytes. */
  private long mCapacityBytes;
  private HashMap<Integer, StorageDir> mDirs;
  /** The lost storage paths that are failed to initialize or lost. */
  private List<String> mLostStorage;

  private DefaultStorageTier(String tierAlias) {
    mTierAlias = tierAlias;
    mTierOrdinal = new WorkerStorageTierAssoc().getOrdinal(tierAlias);
  }

  private void initStorageTier(boolean isMultiTier)
      throws BlockAlreadyExistsException, IOException, WorkerOutOfSpaceException {
    String tmpDir = ServerConfiguration.get(PropertyKey.WORKER_DATA_TMP_FOLDER);
    PropertyKey tierDirPathConf =
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(mTierOrdinal);
    String[] dirPaths = ServerConfiguration.get(tierDirPathConf).split(",");

    for (int i = 0; i < dirPaths.length; i++) {
      dirPaths[i] = CommonUtils.getWorkerDataDirectory(dirPaths[i], ServerConfiguration.global());
    }

    PropertyKey tierDirCapacityConf =
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(mTierOrdinal);
    String rawDirQuota = ServerConfiguration.get(tierDirCapacityConf);
    Preconditions.checkState(rawDirQuota.length() > 0, PreconditionMessage.ERR_TIER_QUOTA_BLANK);
    String[] dirQuotas = rawDirQuota.split(",");

    PropertyKey tierDirMediumConf =
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE.format(mTierOrdinal);
    String rawDirMedium = ServerConfiguration.get(tierDirMediumConf);
    Preconditions.checkState(rawDirMedium.length() > 0,
        "Tier medium type configuration should not be blank");
    String[] dirMedium = rawDirMedium.split(",");

    // Set reserved bytes on directories if tier aligning is enabled.
    long reservedBytes = 0;
    if (isMultiTier
        && ServerConfiguration.getBoolean(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED)) {
      reservedBytes =
          ServerConfiguration.getBytes(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES);
    }

    mDirs = new HashMap<>(dirPaths.length);
    mLostStorage = new ArrayList<>();

    long totalCapacity = 0;
    for (int i = 0; i < dirPaths.length; i++) {
      int index = i >= dirQuotas.length ? dirQuotas.length - 1 : i;
      int mediumTypeindex = i >= dirMedium.length ? dirMedium.length - 1 : i;
      long capacity = FormatUtils.parseSpaceSize(dirQuotas[index]);
      try {
        StorageDir dir = DefaultStorageDir.newStorageDir(this, i, capacity, reservedBytes,
            dirPaths[i], dirMedium[mediumTypeindex]);
        totalCapacity += capacity;
        mDirs.put(i, dir);
      } catch (IOException | InvalidPathException e) {
        LOG.error("Unable to initialize storage directory at {}", dirPaths[i], e);
        mLostStorage.add(dirPaths[i]);
        continue;
      }

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
    if (mTierAlias.equals(Constants.MEDIUM_MEM) && mDirs.size() == 1) {
      checkEnoughMemSpace(mDirs.values().iterator().next());
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
      LOG.warn("Failed to get mount information for verifying memory capacity: {}", e.toString());
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
      if (fsType.equalsIgnoreCase("tmpfs") && size < storageDir.getCapacityBytes()) {
        throw new IllegalStateException(String.format(
            "%s is smaller than the configured size: %s size: %s, configured size: %s",
            fsType, fsType, size, storageDir.getCapacityBytes()));
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
   * @param isMultiTier whether this tier is part of a multi-tier setup
   * @return a new storage tier
   * @throws BlockAlreadyExistsException if the tier already exists
   * @throws WorkerOutOfSpaceException if there is not enough space available
   */
  public static StorageTier newStorageTier(String tierAlias, boolean isMultiTier)
      throws BlockAlreadyExistsException, IOException, WorkerOutOfSpaceException {
    DefaultStorageTier ret = new DefaultStorageTier(tierAlias);
    ret.initStorageTier(isMultiTier);
    return ret;
  }

  @Override
  public int getTierOrdinal() {
    return mTierOrdinal;
  }

  @Override
  public String getTierAlias() {
    return mTierAlias;
  }

  @Override
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  @Override
  public long getAvailableBytes() {
    long availableBytes = 0;
    for (StorageDir dir : mDirs.values()) {
      availableBytes += dir.getAvailableBytes();
    }
    return availableBytes;
  }

  @Override
  @Nullable
  public StorageDir getDir(int dirIndex) {
    return mDirs.get(dirIndex);
  }

  @Override
  public List<StorageDir> getStorageDirs() {
    return new ArrayList<>(mDirs.values());
  }

  @Override
  public List<String> getLostStorage() {
    return new ArrayList<>(mLostStorage);
  }

  @Override
  public void removeStorageDir(StorageDir dir) {
    if (mDirs.remove(dir.getDirIndex()) != null) {
      mCapacityBytes -=  dir.getCapacityBytes();
    }
    mLostStorage.add(dir.getDirPath());
  }
}
