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

package alluxio.cli.validation;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.FormatUtils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Task for validating whether worker tiered storage has enough space.
 */
public final class StorageSpaceValidationTask implements ValidationTask {

  /**
   * Creates a new instance of {@link StorageSpaceValidationTask}
   * for validating tiered storage space.
   */
  public StorageSpaceValidationTask() {
  }

  @Override
  public boolean validate() {
    int numLevel = Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    boolean success = true;

    for (int level = 0; level < numLevel; level++) {
      PropertyKey tierAliasConf =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(level);
      String alias = Configuration.get(tierAliasConf);

      PropertyKey tierDirPathConf =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level);
      String[] dirPaths = Configuration.get(tierDirPathConf).split(",");

      PropertyKey tierDirCapacityConf =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(level);
      String rawDirQuota = Configuration.get(tierDirCapacityConf);
      if (rawDirQuota.length() <= 0) {
        System.err.format("Tier %d: Quota cannot be empty.%n", level);
        return false;
      }

      String[] dirQuotas = rawDirQuota.split(",");

      try {
        Map<String, MountedStorage> storageMap = new HashMap<>();
        File file = new File(dirPaths[0]);
        if (dirPaths.length == 1 && alias.equals("MEM") && !file.exists()) {
          // skip checking if RAM disk is not mounted
          System.out.format("RAM disk is not mounted at %s, skip validation.%n", dirPaths[0]);
          continue;
        }

        for (int i = 0; i < dirPaths.length; i++) {
          int index = i >= dirQuotas.length ? dirQuotas.length - 1 : i;
          long quota = FormatUtils.parseSpaceSize(dirQuotas[index]);
          success &= addDirectoryInfo(dirPaths[i], quota, storageMap);
        }

        for (Map.Entry<String, MountedStorage> storageEntry : storageMap.entrySet()) {
          MountedStorage storage = storageEntry.getValue();
          long capacity = storage.getCapacitySize();
          long used = storage.getWorkspaceSize();
          long available = storage.getAvailableSize();
          if (capacity > used + available) {
            System.err.format(
                "Tier %d: Not enough space on %s. %n"
                    + "Required capacity: %s%n"
                    + "Used in working directory: %s%n"
                    + "Available: %s%n",
                level, storageEntry.getKey(),
                FormatUtils.getSizeFromBytes(capacity),
                FormatUtils.getSizeFromBytes(used),
                FormatUtils.getSizeFromBytes(available));
            success = false;
          }
        }
      } catch (IOException e) {
        System.err.format("Tier %d: Unable to validate available space - %s.%n",
            level, e.getMessage());
        success = false;
      }
    }

    return success;
  }

  private boolean addDirectoryInfo(String path, long quota, Map<String, MountedStorage> storageMap)
      throws IOException {
    File file = new File(path);
    if (!file.exists()) {
      System.err.format("Path %s does not exist.%n", path);
      return false;
    }

    if (!file.isDirectory()) {
      System.err.format("Path %s is not a valid directory.%n", path);
      return false;
    }

    long directorySize = FileUtils.sizeOfDirectory(file);

    FileStore store = Files.getFileStore(Paths.get(path));

    MountedStorage storage = storageMap.get(store.name());
    if (storage == null) {
      storage = new MountedStorage(store);
      storageMap.put(store.name(), storage);
    }

    storage.addCapacitySize(quota);
    storage.addWorkspaceSize(directorySize);
    return true;
  }

  private final class MountedStorage {
    private long mCapacitySize;
    private long mWorkspaceSize;
    private FileStore mFileStore;

    public MountedStorage(FileStore store) {
      mCapacitySize = 0L;
      mWorkspaceSize = 0L;
      mFileStore = store;
    }

    public long getCapacitySize() {
      return mCapacitySize;
    }

    public void addCapacitySize(long capacitySize) {
      mCapacitySize += capacitySize;
    }

    public long getWorkspaceSize() {
      return mWorkspaceSize;
    }

    public void addWorkspaceSize(long workspaceSize) {
      mWorkspaceSize += workspaceSize;
    }

    public long getAvailableSize() throws IOException {
      return mFileStore.getUsableSpace();
    }
  }
}
