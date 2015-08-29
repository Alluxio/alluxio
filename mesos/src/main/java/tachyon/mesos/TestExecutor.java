import java.io.File;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

public class TestExecutor implements Executor {
  @Override
  public void registered(ExecutorDriver driver,
                         ExecutorInfo executorInfo,
                         FrameworkInfo frameworkInfo,
                         SlaveInfo slaveInfo) {
    System.out.println("Registered executor on " + slaveInfo.getHostname());
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo executorInfo) {}

  @Override
  public void disconnected(ExecutorDriver driver) {}

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    new Thread() { public void run() {
      try {
        TaskStatus status = TaskStatus.newBuilder()
            .setTaskId(task.getTaskId())
            .setState(TaskState.TASK_RUNNING).build();

        driver.sendStatusUpdate(status);

        System.out.println("Running task " + task.getTaskId().getValue());

        // This is where one would perform the requested task.

        status = TaskStatus.newBuilder()
            .setTaskId(task.getTaskId())
            .setState(TaskState.TASK_FINISHED).build();

        driver.sendStatusUpdate(status);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }}.start();
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {}

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {}

  @Override
  public void shutdown(ExecutorDriver driver) {}

  @Override
  public void error(ExecutorDriver driver, String message) {}

  public static void main(String[] args) throws Exception {
    MesosExecutorDriver driver = new MesosExecutorDriver(new TestExecutor());
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }
}
