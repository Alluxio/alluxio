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

package tachyon.perf.benchmark.simpleread;

import java.io.IOException;

import tachyon.perf.PerfConstants;
import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.benchmark.SimpleTask;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.fs.PerfFS;

public class SimpleReadTask extends SimpleTask {
  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    try {
      PerfFS fs = PerfConstants.getFileSystem();
      String readDir = PerfConf.get().WORK_DIR + "/simple-read-write/" + mId;
      if (!fs.exists(readDir)) {
        throw new IOException("No data to read at " + readDir);
      }
      mTaskConf.addProperty("read.dir", readDir);
      LOG.info("Read dir " + readDir);
    } catch (IOException e) {
      LOG.error("Failed to setup task " + mId, e);
      return false;
    }
    return true;
  }
}
