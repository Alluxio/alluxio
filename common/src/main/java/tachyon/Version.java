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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import tachyon.conf.TachyonConf;

/**
 * The version of the current build.
 */
public final class Version {
  public static final String VERSION;
  private static Map<String, Set<Long>> sCompatibleVersions;

  private Version() {}

  static {
    TachyonConf tachyonConf = new TachyonConf();
    VERSION = tachyonConf.get(Constants.TACHYON_VERSION);

    sCompatibleVersions = new HashMap<String, Set<Long>>();

    Set<Long> blockMasterVersions = new HashSet<Long>();
    blockMasterVersions.add(Constants.BLOCK_MASTER_SERVICE_VERSION);
    sCompatibleVersions.put(Constants.BLOCK_MASTER_SERVICE_NAME, blockMasterVersions);

    Set<Long> fileSystemMasterVersions = new HashSet<Long>();
    fileSystemMasterVersions.add(Constants.FILE_SYSTEM_MASTER_SERVICE_VERSION);
    sCompatibleVersions.put(Constants.FILE_SYSTEM_MASTER_SERVICE_NAME, fileSystemMasterVersions);

    Set<Long> lineageMasterVersions = new HashSet<Long>();
    lineageMasterVersions.add(Constants.LINEAGE_MASTER_SERVICE_VERSION);
    sCompatibleVersions.put(Constants.LINEAGE_MASTER_SERVICE_NAME, lineageMasterVersions);

    Set<Long> rawTableMasterVersions = new HashSet<Long>();
    rawTableMasterVersions.add(Constants.RAW_TABLE_MASTER_SERVICE_VERSION);
    sCompatibleVersions.put(Constants.RAW_TABLE_MASTER_SERVICE_NAME, rawTableMasterVersions);
  }

  // Relative path to Tachyon target jar
  public static final String TACHYON_JAR = "target/tachyon-" + VERSION
      + "-jar-with-dependencies.jar";

  /**
   * @param serviceName the service name
   * @return the set of service versions the implementation supports
   */
  public static Set<Long> getCompatibleVersions(String serviceName) {
    return sCompatibleVersions.get(serviceName);
  }

  public static void main(String[] args) {
    System.out.println("Tachyon version: " + VERSION);
  }
}
