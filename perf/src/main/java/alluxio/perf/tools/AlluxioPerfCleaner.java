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

package alluxio.perf.tools;

import java.io.IOException;

import alluxio.perf.PerfConstants;
import alluxio.perf.conf.PerfConf;
import alluxio.perf.fs.PerfFS;

import alluxio.Configuration;
import alluxio.Constants;

/**
 * A tool to clean the workspace on Alluxio/HDFS.
 */
public class AlluxioPerfCleaner {
  public static void main(String[] args) {
    String alluxioAddress = new Configuration().get(Constants.MASTER_ADDRESS);
    String arg = args[0];
    try {
      PerfFS fs = PerfConstants.getFileSystem(arg);
      fs.delete(PerfConf.get().WORK_DIR, true);
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Failed to clean " + arg + " workspace " + PerfConf.get().WORK_DIR + " on "
          + alluxioAddress);
    }
    System.out.println(
        "Clean the " + arg + " workspace " + PerfConf.get().WORK_DIR + " on " + alluxioAddress);
  }
}
