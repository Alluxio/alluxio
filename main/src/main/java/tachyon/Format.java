/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.IOException;

import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;

/**
 * Format Tachyon File System.
 */
public class Format {
  private static final String USAGE = "java -cp target/tachyon-" + Version.VERSION
      + "-jar-with-dependencies.jar tachyon.Format <MASTER/WORKER>";

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.out.println(USAGE);
      System.exit(-1);
    }

    if (args[0].toUpperCase().equals("MASTER")) {
      MasterConf masterConf = MasterConf.get();
      String folder = masterConf.JOURNAL_FOLDER;
      UnderFileSystem ufs = UnderFileSystem.get(folder);
      System.out.println("Formatting JOURNAL_FOLDER: " + folder);
      if (ufs.exists(folder) && !ufs.delete(folder, true)) {
        System.out.println("Failed to remove JOURNAL_FOLDER: " + folder);
      }
      if (!ufs.mkdirs(folder, true)) {
        System.out.println("Failed to create JOURNAL_FOLDER: " + folder);
      }

      CommonConf commonConf = CommonConf.get();
      folder = commonConf.UNDERFS_DATA_FOLDER;
      ufs = UnderFileSystem.get(folder);
      System.out.println("Formatting UNDERFS_DATA_FOLDER: " + folder);
      ufs.delete(folder, true);
      if (!ufs.mkdirs(folder, true)) {
        System.out.println("Failed to create UNDERFS_DATA_FOLDER: " + folder);
      }
      commonConf = CommonConf.get();
      folder = commonConf.UNDERFS_WORKERS_FOLDER;
      System.out.println("Formatting UNDERFS_WORKERS_FOLDER: " + folder);
      ufs.delete(folder, true);
      if (!ufs.mkdirs(folder, true)) {
        System.out.println("Failed to create UNDERFS_WORKERS_FOLDER: " + folder);
      }

      CommonUtils.touch(folder + "/" + masterConf.FORMAT_FILE_PREFIX + System.currentTimeMillis());
    } else if (args[0].toUpperCase().equals("WORKER")) {
      WorkerConf workerConf = WorkerConf.get();
      String localFolder = workerConf.DATA_FOLDER;
      UnderFileSystem ufs = UnderFileSystem.get(localFolder);
      System.out.println("Removing local data under folder: " + localFolder);
      if (ufs.exists(localFolder)) {
        String[] files = ufs.list(localFolder);
        for (String file : files) {
          ufs.delete(localFolder + "/" + file, true);
        }
      }
    } else {
      System.out.println(USAGE);
      System.exit(-1);
    }
  }
}