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

import org.apache.log4j.Logger;

import alluxio.perf.basic.PerfTask;
import alluxio.perf.basic.PerfTaskContext;
import alluxio.perf.basic.TaskConfiguration;
import alluxio.perf.basic.TestCase;

/**
 * Entry point for the Alluxio-Perf Slave program.
 */
public class AlluxioPerfSlave {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static void main(String[] args) {
    if (args.length < 4) {
      LOG.error("Wrong program arguments. Should be <NodeName> <TaskId> <TotalTasks> <TestCase>"
          + "See more in bin/alluxio-perf");
      System.exit(-1);
    } else {
      LOG.info("starting slave with arguments " + args);
    }

    String nodeName = null;
    int taskId = -1;
    int totalTasks = -1;
    String testCase = null;
    try {
      nodeName = args[0];
      taskId = Integer.parseInt(args[1]);
      totalTasks = Integer.parseInt(args[2]);
      testCase = args[3];
    } catch (Exception e) {
      LOG.error("Failed to parse the input args", e);
      System.exit(-1);
    }

    try {
      TaskConfiguration taskConf = TaskConfiguration.get(testCase, true);
      PerfTask task = TestCase.get().getTaskClass(testCase);
      task.initialSet(taskId, totalTasks, nodeName, taskConf, testCase);
      PerfTaskContext taskContext = TestCase.get().getTaskContextClass(testCase);
      taskContext.initialSet(taskId, nodeName, testCase, taskConf);

      MasterClient masterClient = new MasterClient();
      while (!masterClient.slave_register(taskId, nodeName, task.getCleanupDir())) {
        Thread.sleep(1000);
      }

      masterClient.slave_ready(taskId, nodeName, task.setup(taskContext));

      while (!masterClient.slave_canRun(taskId, nodeName)) {
        Thread.sleep(100);
      }
      if (!task.run(taskContext)) {
        masterClient.slave_finish(taskId, nodeName, false);
      } else {
        masterClient.slave_finish(taskId, nodeName,
            task.cleanup(taskContext) & taskContext.getSuccess());
      }
    } catch (Exception e) {
      LOG.error("Error in task", e);
    }
  }
}
