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

package alluxio.perf.benchmark.simpleread;

import java.io.IOException;

import alluxio.perf.PerfConstants;
import alluxio.perf.basic.PerfTaskContext;
import alluxio.perf.benchmark.SimpleTask;
import alluxio.perf.conf.PerfConf;
import alluxio.perf.fs.PerfFS;

public class SimpleReadTask extends SimpleTask {
  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    try {
      PerfFS fs = PerfConstants.getFileSystem();
      // Potentially read from another task's directory.
      int offset = mTaskConf.getIntProperty("taskid.offset");
      String readDir = PerfConf.get().WORK_DIR + "/simple-read-write/"
          + ((mId + offset) % mTotalTasks);
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
