package tachyon.perf;

import org.apache.log4j.Logger;

import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.basic.TaskType;

/**
 * Entry point for the Tachyon-Perf Slave program.
 */
public class TachyonPerfSlave {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static void main(String[] args) {
    if (args.length < 3) {
      LOG.error("Wrong program arguments. Should be <NodeName> <TaskId> <TaskType>"
          + "See more in bin/tachyon-perf");
      System.exit(-1);
    }

    String nodeName = null;
    int taskId = -1;
    String taskType = null;
    try {
      nodeName = args[0];
      taskId = Integer.parseInt(args[1]);
      taskType = args[2];
    } catch (Exception e) {
      LOG.error("Failed to parse the input args", e);
      System.exit(-1);
    }

    try {
      TaskConfiguration taskConf = TaskConfiguration.get(taskType, true);
      PerfTask task = TaskType.get().getTaskClass(taskType);
      task.initialSet(taskId, nodeName, taskConf, taskType);
      PerfTaskContext taskContext = TaskType.get().getTaskContextClass(taskType);
      taskContext.initialSet(taskId, nodeName, taskType, taskConf);

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
