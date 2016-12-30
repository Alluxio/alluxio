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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.PropertyKeyFormat;
import alluxio.RuntimeConstants;
import alluxio.ServerUtils;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Formats the Alluxio file system.
 */
@ThreadSafe
public final class Format {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String USAGE = String.format("java -cp %s %s <MASTER/WORKER>",
      RuntimeConstants.ALLUXIO_JAR, Format.class.getCanonicalName());

  private static boolean formatFolder(String name, String folder) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.get(folder);
    LOG.info("Formatting {}:{}", name, folder);
    if (ufs.isDirectory(folder)) {
      for (UnderFileStatus p : ufs.listStatus(folder)) {
        String childPath = PathUtils.concatPath(folder, p.getName());
        boolean failedToDelete;
        if (p.isDirectory()) {
          failedToDelete = !ufs.deleteDirectory(childPath,
              DeleteOptions.defaults().setRecursive(true));
        } else {
          failedToDelete = !ufs.deleteFile(childPath);
        }
        if (failedToDelete) {
          LOG.info("Failed to delete {}", childPath);
          return false;
        }
      }
    } else if (!ufs.mkdirs(folder)) {
      LOG.info("Failed to create {}:{}", name, folder);
      return false;
    }
    return true;
  }

  /**
   * Formats the Alluxio file system.
   *
   * @param args either {@code MASTER} or {@code WORKER}
   * @throws IOException if a non-Alluxio related exception occurs
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      LOG.info(USAGE);
      System.exit(-1);
    }
    try {
      format(args[0]);
    } catch (Exception e) {
      LOG.error("Failed to format", e);
      System.exit(-1);
    }
    System.exit(0);
  }

  public static void format(String mode) throws IOException {
    if ("MASTER".equalsIgnoreCase(mode)) {
      String masterJournal =
          Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
      if (!formatFolder("JOURNAL_FOLDER", masterJournal)) {
        throw new RuntimeException("Failed to format root journal folder");
      }

      for (String masterServiceName : ServerUtils.getMasterServiceNames()) {
        String folderName = masterServiceName + "_JOURNAL_FOLDER";
        if (!formatFolder(folderName,
            PathUtils.concatPath(masterJournal, masterServiceName))) {
          throw new RuntimeException(String.format("Failed to format %s", folderName));
        }
      }

      // A journal folder is thought to be formatted only when a file with the specific name is
      // present under the folder.
      UnderFileSystemUtils.touch(PathUtils
          .concatPath(masterJournal, Constants.FORMAT_FILE_PREFIX + System.currentTimeMillis()));
    } else if ("WORKER".equalsIgnoreCase(mode)) {
      String workerDataFolder = Configuration.get(PropertyKey.WORKER_DATA_FOLDER);
      int storageLevels = Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
      for (int level = 0; level < storageLevels; level++) {
        PropertyKey tierLevelDirPath =
            PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(level);
        String[] dirPaths = Configuration.get(tierLevelDirPath).split(",");
        String name = "TIER_" + level + "_DIR_PATH";
        for (String dirPath : dirPaths) {
          String dirWorkerDataFolder = PathUtils.concatPath(dirPath.trim(), workerDataFolder);
          UnderFileSystem ufs = UnderFileSystem.Factory.get(dirWorkerDataFolder);
          if (ufs.isDirectory(dirWorkerDataFolder)) {
            if (!formatFolder(name, dirWorkerDataFolder)) {
              throw new RuntimeException(String.format("Failed to format worker data folder %s",
                  dirWorkerDataFolder));
            }
          }
        }
      }
    } else {
      LOG.info(USAGE);
      throw new RuntimeException(String.format("Unrecognized format mode: %s", mode));
    }
  }

  private Format() {}  // prevent instantiation
}
