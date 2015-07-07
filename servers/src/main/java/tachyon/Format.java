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

package tachyon;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.util.UnderFileSystemUtils;

/**
 * Format Tachyon File System.
 */
public class Format {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String USAGE = "java -cp target/tachyon-" + Version.VERSION
      + "-jar-with-dependencies.jar tachyon.Format <MASTER/WORKER>";

  private static boolean formatFolder(String name, String folder, TachyonConf tachyonConf)
      throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(folder, tachyonConf);
    LOG.info("Formatting {}:{}", name, folder);
    if (ufs.exists(folder)) {
      for (String file : ufs.list(folder)) {
        if (!ufs.delete(CommonUtils.concatPath(folder, file), true)) {
          LOG.info("Failed to remove {}:{}", name, file);
          return false;
        }
      }
    } else if (!ufs.mkdirs(folder, true)) {
      LOG.info("Failed to create {}:{}", name, folder);
      return false;
    }
    return true;
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      LOG.info(USAGE);
      System.exit(-1);
    }

    TachyonConf tachyonConf = new TachyonConf();

    if (args[0].toUpperCase().equals("MASTER")) {

      String masterJournal =
          tachyonConf.get(Constants.MASTER_JOURNAL_FOLDER, Constants.DEFAULT_JOURNAL_FOLDER);
      if (!formatFolder("JOURNAL_FOLDER", masterJournal, tachyonConf)) {
        System.exit(-1);
      }

      String tachyonHome = tachyonConf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);
      String ufsAddress =
          tachyonConf.get(Constants.UNDERFS_ADDRESS, tachyonHome + "/underFSStorage");
      String ufsDataFolder =
          tachyonConf.get(Constants.UNDERFS_DATA_FOLDER, ufsAddress + "/tachyon/data");
      String ufsWorkerFolder =
          tachyonConf.get(Constants.UNDERFS_WORKERS_FOLDER, ufsAddress + "/tachyon/workers");
      if (!formatFolder("UNDERFS_DATA_FOLDER", ufsDataFolder, tachyonConf)
          || !formatFolder("UNDERFS_WORKERS_FOLDER", ufsWorkerFolder, tachyonConf)) {
        System.exit(-1);
      }

      UnderFileSystemUtils.touch(
          masterJournal + Constants.FORMAT_FILE_PREFIX + System.currentTimeMillis(), tachyonConf);
    } else if (args[0].toUpperCase().equals("WORKER")) {
      String workerDataFolder =
          tachyonConf.get(Constants.WORKER_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER);
      int maxStorageLevels = tachyonConf.getInt(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, 1);
      for (int level = 0; level < maxStorageLevels; level ++) {
        String tierLevelDirPath =
            String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, level);
        String[] dirPaths = tachyonConf.get(tierLevelDirPath, "/mnt/ramdisk").split(",");
        String name = "TIER_" + level + "_DIR_PATH";
        for (String dirPath : dirPaths) {
          String dirWorkerDataFolder = CommonUtils.concatPath(dirPath.trim(), workerDataFolder);
          UnderFileSystem ufs = UnderFileSystem.get(dirWorkerDataFolder, tachyonConf);
          if (ufs.exists(dirWorkerDataFolder)) {
            if (!formatFolder(name, dirWorkerDataFolder, tachyonConf)) {
              System.exit(-1);
            }
          }
        }
      }
    } else {
      LOG.info(USAGE);
      System.exit(-1);
    }
  }
}
