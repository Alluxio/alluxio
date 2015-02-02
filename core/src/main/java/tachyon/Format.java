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

import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;

/**
 * Format Tachyon File System.
 */
public class Format {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String USAGE = "java -cp target/tachyon-" + Version.VERSION
      + "-jar-with-dependencies.jar tachyon.Format <MASTER/WORKER>";

  private static boolean formatFolder(String name, String folder) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(folder);
    LOG.info("Formatting {}:{}", name, folder);
    if (ufs.exists(folder) && !ufs.delete(folder, true)) {
      LOG.info("Failed to remove {}:{}", name, folder);
      return false;
    }
    if (!ufs.mkdirs(folder, true)) {
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

    if (args[0].toUpperCase().equals("MASTER")) {
      MasterConf masterConf = MasterConf.get();

      if (!formatFolder("JOURNAL_FOLDER", masterConf.JOURNAL_FOLDER)) {
        System.exit(-1);
      }

      CommonConf commonConf = CommonConf.get();
      if (!formatFolder("UNDERFS_DATA_FOLDER", commonConf.UNDERFS_DATA_FOLDER)
          || !formatFolder("UNDERFS_WORKERS_FOLDER", commonConf.UNDERFS_WORKERS_FOLDER)) {
        System.exit(-1);
      }

      CommonUtils.touch(masterConf.JOURNAL_FOLDER + masterConf.FORMAT_FILE_PREFIX
          + System.currentTimeMillis());
    } else if (args[0].toUpperCase().equals("WORKER")) {
      WorkerConf workerConf = WorkerConf.get();
      for (int level = 0; level < workerConf.STORAGE_LEVELS; level ++) {
        String[] dirPaths = workerConf.STORAGE_TIER_DIRS[level].split(",");
        for (int i = 0; i < dirPaths.length; i ++) {
          String dataPath = CommonUtils.concat(dirPaths[i].trim(), workerConf.DATA_FOLDER);
          UnderFileSystem ufs = UnderFileSystem.get(dataPath);
          LOG.info("Removing data under folder: {}", dataPath);
          if (ufs.exists(dataPath)) {
            String[] files = ufs.list(dataPath);
            for (String file : files) {
              ufs.delete(CommonUtils.concat(dataPath, file), true);
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
