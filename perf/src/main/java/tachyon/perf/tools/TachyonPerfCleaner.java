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

package tachyon.perf.tools;

import java.io.IOException;

import tachyon.conf.TachyonConf;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.fs.PerfFS;

/**
 * A tool to clean the workspace on Tachyon.
 */
public class TachyonPerfCleaner {
  public static void main(String[] args) {
    String tachyonAddress = new TachyonConf().get(tachyon.Constants.MASTER_ADDRESS, "");
    try {
      PerfFS fs = PerfConstants.getFileSystem();
      fs.delete(PerfConf.get().WORK_DIR, true);
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Failed to clean workspace " + PerfConf.get().WORK_DIR + " on "
          + tachyonAddress);
    }
    System.out.println("Clean the workspace " + PerfConf.get().WORK_DIR + " on " + tachyonAddress);
  }
}
