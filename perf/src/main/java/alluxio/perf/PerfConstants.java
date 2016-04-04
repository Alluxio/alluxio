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

package alluxio.perf;

import java.io.IOException;

import alluxio.perf.fs.HDFSPerfFS;
import alluxio.perf.fs.PerfFS;
import alluxio.perf.fs.THCIPerfFS;
import alluxio.perf.fs.AlluxioPerfFS;


/**
 * Alluxio-Perf constants
 */
public class PerfConstants {
  public static final String PERF_LOGGER_TYPE = System.getProperty("alluxio.perf.logger.type", "");
  public static final String PERF_CONTEXT_FILE_NAME_PREFIX = "context";
  public static final String PERF_UFS = System.getProperty("alluxio.perf.ufs", "Alluxio");

  public static PerfFS getFileSystem() throws IOException {
    return getFileSystem(PERF_UFS);
  }

  public static PerfFS getFileSystem(String type) throws IOException {
    if (type.equalsIgnoreCase("Alluxio")) {
      return AlluxioPerfFS.get();
    }
    if (type.equalsIgnoreCase("THCI")) {
      return THCIPerfFS.get();
    }
    if (type.equalsIgnoreCase("HDFS")) {
      return HDFSPerfFS.get();
    }
    // defaults to Alluxio FS
    return AlluxioPerfFS.get();
  }
}
