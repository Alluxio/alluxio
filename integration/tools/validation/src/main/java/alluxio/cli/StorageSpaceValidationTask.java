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

package alluxio.cli;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.FormatUtils;
import alluxio.util.ShellUtils;

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
public final class StorageSpaceValidationTask extends AbstractValidationTask {
  private final AlluxioConfiguration mConf;

  /**
   * Creates a new instance of {@link StorageSpaceValidationTask}
   * for validating tiered storage space.
   * @param conf configuration
   */
  public StorageSpaceValidationTask(AlluxioConfiguration conf) {
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateStorageSpace";
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionsMap) {
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();

    int numLevel = mConf.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    boolean success = true;
    for (int level = 0; level < numLevel; level++) {
      PropertyKey tierAliasConf =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(level);
      String alias = mConf.get(tierAliasConf);

      PropertyKey tierDirPathConf =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level);
      String[] dirPaths = mConf.get(tierDirPathConf).split(",");

      PropertyKey tierDirCapacityConf =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(level);
      String rawDirQuota = mConf.get(tierDirCapacityConf);
      if (rawDirQuota.isEmpty()) {
        msg.append(String.format("Tier %d: Quota cannot be empty.%n", level));
        advice.append(String.format("Please check your setting for %s.%n",
                tierDirCapacityConf.toString()));
        return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
                msg.toString(), advice.toString());
      }

      String[] dirQuotas = rawDirQuota.split(",");

      try {
        Map<String, MountedStorage> storageMap = new HashMap<>();
        File file = new File(dirPaths[0]);
        if (dirPaths.length == 1 && alias.equals(Constants.MEDIUM_MEM) && !file.exists()) {
          msg.append(String.format("RAM disk is not mounted at %s, skip validation.%n",
                  dirPaths[0]));
          continue;
        }

        boolean hasRamfsLocation = false;
        for (int i = 0; i < dirPaths.length; i++) {
          int index = i >= dirQuotas.length ? dirQuotas.length - 1 : i;
          if (ShellUtils.isMountingPoint(dirPaths[i], new String[] {"ramfs"})) {
            msg.append(String.format("ramfs mounted at %s does not report space information,"
                + " skip validation.%n", dirPaths[i]));
            hasRamfsLocation = true;
            break;
          }
          long quota = FormatUtils.parseSpaceSize(dirQuotas[index]);
          success &= addDirectoryInfo(dirPaths[i], quota, storageMap);
        }
        if (hasRamfsLocation) {
          continue;
        }

        for (Map.Entry<String, MountedStorage> storageEntry : storageMap.entrySet()) {
          MountedStorage storage = storageEntry.getValue();
          long quota = storage.getDesiredQuotaSizeBytes();
          long used = storage.getUsedTieredStorageSizeBytes();
          long available = storage.getAvailableSizeBytes();
          StringBuilder builder = new StringBuilder();
          for (Map.Entry<String, Long> directoryQuota : storage.getDirectoryQuotas().entrySet()) {
            builder.append(String.format("- Quota for %s: %s%n", directoryQuota.getKey(),
                FormatUtils.getSizeFromBytes(directoryQuota.getValue())));
          }
          if (quota > used + available) {
            msg.append(String.format(
                "Tier %d: Not enough space on %s. %n"
                    + "Total desired quota: %s%n"
                    + "%s"
                    + "Used in tiered storage: %s%n"
                    + "Available: %s (Additional %s free space required).%n",
                level, storageEntry.getKey(),
                FormatUtils.getSizeFromBytes(quota),
                builder.toString(),
                FormatUtils.getSizeFromBytes(used),
                FormatUtils.getSizeFromBytes(available),
                FormatUtils.getSizeFromBytes(quota - used - available)));
            advice.append(String.format("Please check your quota setting for tier %s.%n", level));
            success = false;
          }
        }
      } catch (IOException e) {
        msg.append(String.format("Tier %d: Unable to validate available space - %s.%n",
            level, e.getMessage()));
        msg.append(ValidationUtils.getErrorInfo(e));
        advice.append(String.format("Please check your path for tier %s.%n", level));
        success = false;
      }
    }

    ValidationUtils.State state = success ? ValidationUtils.State.OK
            : ValidationUtils.State.WARNING;
    return new ValidationTaskResult(state, getName(), msg.toString(), advice.toString());
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

    // gets mounted FileStore that backs the directory of the given path
    FileStore store = Files.getFileStore(Paths.get(path));

    MountedStorage storage = storageMap.get(store.name());
    if (storage == null) {
      storage = new MountedStorage(store);
      storageMap.put(store.name(), storage);
    }

    storage.addDirectoryInfo(path, quota, directorySize);
    return true;
  }

  private final class MountedStorage {
    private long mDesiredQuotaSizeBytes;
    private long mUsedTieredStorageSizeBytes;
    private FileStore mFileStore;
    private final Map<String, Long> mDirectoryQuotas;

    public MountedStorage(FileStore store) {
      mDesiredQuotaSizeBytes = 0L;
      mUsedTieredStorageSizeBytes = 0L;
      mFileStore = store;
      mDirectoryQuotas = new HashMap<>();
    }

    public long getDesiredQuotaSizeBytes() {
      return mDesiredQuotaSizeBytes;
    }

    public long getUsedTieredStorageSizeBytes() {
      return mUsedTieredStorageSizeBytes;
    }

    public long getAvailableSizeBytes() throws IOException {
      return mFileStore.getUsableSpace();
    }

    public Map<String, Long> getDirectoryQuotas() {
      return mDirectoryQuotas;
    }

    public void addDirectoryInfo(String path, long quota, long directorySize) {
      mDirectoryQuotas.put(path, quota);
      mUsedTieredStorageSizeBytes += directorySize;
      mDesiredQuotaSizeBytes += quota;
    }
  }
}
