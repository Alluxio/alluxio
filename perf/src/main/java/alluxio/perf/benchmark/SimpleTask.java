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

package alluxio.perf.benchmark;

import alluxio.perf.basic.PerfTask;
import alluxio.perf.basic.PerfTaskContext;
import alluxio.perf.conf.PerfConf;

/**
 * An simple example to extend PerfTask. It can be used to quickly implement a new benchmark.
 */
public class SimpleTask extends PerfTask {
  @Override
  protected boolean cleanupTask(PerfTaskContext taskContext) {
    return true;
  }

  @Override
  public String getCleanupDir() {
    return null;
  }

  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    String workspacePath = PerfConf.get().WORK_DIR;
    LOG.info("Perf workspace " + workspacePath);
    return true;
  }
}
