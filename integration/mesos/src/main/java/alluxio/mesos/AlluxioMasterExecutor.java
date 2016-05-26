/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.mesos;

import alluxio.cli.Format;
import alluxio.master.AlluxioMaster;
import alluxio.underfs.UnderFileSystemRegistry;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;

import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link AlluxioMasterExecutor} is an implementation of a Mesos executor responsible for
 * starting the Alluxio master.
 */
@ThreadSafe
public class AlluxioMasterExecutor implements Executor {

  /**
   * Creates a new {@link AlluxioMasterExecutor}.
   */
  public AlluxioMasterExecutor() {}

  @Override
  public void disconnected(ExecutorDriver driver) {
    System.out.println("Executor has disconnected from the Mesos slave.");
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    System.out.println("A fatal error has occurred: " + message + ".");
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {
    System.out.println("Received a framework message.");
  }

  @Override
  public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
    System.out.println("Killing task " + taskId.getValue() + ".");
    // TODO(jiri): Implement.
  }

  @Override
  public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {
    new Thread() {
      public void run() {
        try {
          Protos.TaskStatus status =
              Protos.TaskStatus.newBuilder().setTaskId(task.getTaskId())
                  .setState(Protos.TaskState.TASK_RUNNING).build();

          driver.sendStatusUpdate(status);

          System.out.println("Launching task " + task.getTaskId().getValue());

          Thread.currentThread().setContextClassLoader(
              UnderFileSystemRegistry.class.getClassLoader());

          // TODO(jiri): Consider handling Format.main() failures gracefully.
          Format.main(new String[] {"master"});
          AlluxioMaster.main(new String[] {});

          status =
              Protos.TaskStatus.newBuilder().setTaskId(task.getTaskId())
                  .setState(Protos.TaskState.TASK_FINISHED).build();

          driver.sendStatusUpdate(status);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
  }

  @Override
  public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo,
      Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
    System.out.println("Registered executor " + executorInfo.getName() + " with "
        + slaveInfo.getHostname() + " through framework " + frameworkInfo.getName() + ".");
  }

  @Override
  public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
    System.out.println("Re-registered executor with " + slaveInfo.getHostname() + ".");
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    System.out.println("Shutting down.");
    // TODO(jiri): Implement.
  }

  /**
   * Starts the Alluxio master executor.
   *
   * @param args command-line arguments
   * @throws Exception if the executor encounters an unrecoverable error
   */
  public static void main(String[] args) throws Exception {
    MesosExecutorDriver driver = new MesosExecutorDriver(new AlluxioMasterExecutor());
    System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
  }
}
