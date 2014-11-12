package tachyon.perf;

import org.apache.log4j.Logger;

import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.basic.TestCase;

/**
 * Entry point for the Tachyon-Perf Slave program.
 */
public class TachyonPerfSlave {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static void main(String[] args) {
    if (args.length < 3) {
      LOG.error("Wrong program arguments. Should be <NodeName> <TaskId> <TestCase>"
          + "See more in bin/tachyon-perf");
      System.exit(-1);
    }

    String nodeName = null;
    int taskId = -1;
    String testCase = null;
    try {
      nodeName = args[0];
      taskId = Integer.parseInt(args[1]);
      testCase = args[2];
    } catch (Exception e) {
      LOG.error("Failed to parse the input args", e);
      System.exit(-1);
    }

    try {
      TaskConfiguration taskConf = TaskConfiguration.get(testCase, true);
      PerfTask task = TestCase.get().getTaskClass(testCase);
      task.initialSet(taskId, nodeName, taskConf, testCase);
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
