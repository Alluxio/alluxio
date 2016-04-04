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

package alluxio.perf.benchmark.massive;

import alluxio.perf.basic.PerfTaskContext;
import alluxio.perf.benchmark.SimpleTask;
import alluxio.perf.conf.PerfConf;

public class MassiveTask extends SimpleTask {
  @Override
  public String getCleanupDir() {
    return PerfConf.get().WORK_DIR + "/massive";
  }

  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    String workDir = PerfConf.get().WORK_DIR + "/massive";
    mTaskConf.addProperty("work.dir", workDir);
    LOG.info("Work dir " + workDir);
    return true;
  }
}
