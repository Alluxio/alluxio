/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
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
import tachyon.util.CommonUtils;

/**
 * Format Tachyon File System.
 */
public class Format {
  public static void main(String[] args) throws IOException {
    if (args.length != 0) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION +
          "-jar-with-dependencies.jar tachyon.Format");
      System.exit(-1);
    }

    MasterConf masterConf = MasterConf.get();
    UnderFileSystem ufs = UnderFileSystem.get(masterConf.JOURNAL_FOLDER);
    System.out.println("Deleting " + masterConf.JOURNAL_FOLDER);
    if (ufs.exists(masterConf.JOURNAL_FOLDER) && !ufs.delete(masterConf.JOURNAL_FOLDER, true)) {
      System.out.println("Failed to remove " + masterConf.JOURNAL_FOLDER);
    }
    ufs.mkdirs(masterConf.JOURNAL_FOLDER, true);
    CommonUtils.touch(masterConf.JOURNAL_FOLDER + "/" + masterConf.FORMAT_FILE_PREFIX 
        + System.currentTimeMillis());

    CommonConf commonConf = CommonConf.get();
    String folder = commonConf.UNDERFS_DATA_FOLDER;
    ufs = UnderFileSystem.get(folder);
    System.out.println("Formatting " + folder);
    ufs.delete(folder, true);
    if (!ufs.mkdirs(folder, true)) {
      System.out.println("Failed to create " + folder);
    }

    folder = commonConf.UNDERFS_WORKERS_FOLDER;
    System.out.println("Formatting " + folder);
    ufs.delete(folder, true);
    if (!ufs.mkdirs(folder, true)) {
      System.out.println("Failed to create " + folder);
    }
  }
}