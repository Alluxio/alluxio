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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
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
import com.google.common.collect.ImmutableList;
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

  private DefaultStorageTier(String tierAlias, int tierOrdinal) {
    mTierAlias = tierAlias;
    mTierOrdinal = tierOrdinal;
  }

  private void initStorageTier(boolean isMultiTier)
      throws WorkerOutOfSpaceException {
    String tmpDir = ServerConfiguration.getString(PropertyKey.WORKER_DATA_TMP_FOLDER);
    PropertyKey tierDirPathConf =
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(mTierOrdinal);
    List<String> dirPaths = ServerConfiguration.getList(tierDirPathConf).stream()
        .map(path -> CommonUtils.getWorkerDataDirectory(path, ServerConfiguration.global()))
        .collect(ImmutableList.toImmutableList());

    PropertyKey tierDirCapacityConf =
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(mTierOrdinal);
    List<String> dirQuotas = ServerConfiguration.getList(tierDirCapacityConf);
    Preconditions.checkState(dirQuotas.size() > 0, PreconditionMessage.ERR_TIER_QUOTA_BLANK);

    PropertyKey tierDirMediumConf =
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE.format(mTierOrdinal);
    List<String> dirMedium = ServerConfiguration.getList(tierDirMediumConf);
    Preconditions.checkState(dirMedium.size() > 0,
        "Tier medium type configuration should not be blank");

    // Set reserved bytes on directories if tier aligning is enabled.
    long reservedBytes = 0;
    if (isMultiTier
        && ServerConfiguration.getBoolean(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED)) {
      reservedBytes =
          ServerConfiguration.getBytes(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES);
    }

    mDirs = new HashMap<>(dirPaths.size());
    mLostStorage = new ArrayList<>();

    long totalCapacity = 0;
    for (int i = 0; i < dirPaths.size(); i++) {
      int index = i >= dirQuotas.size() ? dirQuotas.size() - 1 : i;
      int mediumTypeindex = i >= dirMedium.size() ? dirMedium.size() - 1 : i;
      long capacity = FormatUtils.parseSpaceSize(dirQuotas.get(index));
      try {
        StorageDir dir = DefaultStorageDir.newStorageDir(this, i, capacity, reservedBytes,
            dirPaths.get(i), dirMedium.get(mediumTypeindex));
        totalCapacity += capacity;
        mDirs.put(i, dir);
      } catch (IOException e) {
        LOG.error("Unable to initialize storage directory at {}", dirPaths.get(i), e);
        mLostStorage.add(dirPaths.get(i));
        continue;
      }

      // Delete tmp directory.
      String tmpDirPath = PathUtils.concatPath(dirPaths.get(i), tmpDir);
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
   * @param tierOrdinal the tier ordinal
   * @param isMultiTier whether this tier is part of a multi-tier setup
   * @return a new storage tier
   * @throws WorkerOutOfSpaceException if there is not enough space available
   */
  public static StorageTier newStorageTier(String tierAlias, int tierOrdinal, boolean isMultiTier)
      throws WorkerOutOfSpaceException {
    DefaultStorageTier ret = new DefaultStorageTier(tierAlias, tierOrdinal);
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
