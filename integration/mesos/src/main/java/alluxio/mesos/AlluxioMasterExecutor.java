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
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link AlluxioMasterExecutor} is an implementation of a Mesos executor responsible for
 * starting the Alluxio master.
 */
@ThreadSafe
public class AlluxioMasterExecutor implements Executor {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterExecutor.class);

  /**
   * Creates a new {@link AlluxioMasterExecutor}.
   */
  public AlluxioMasterExecutor() {}

  @Override
  public void disconnected(ExecutorDriver driver) {
    LOG.info("Executor has disconnected from the Mesos slave");
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    LOG.error("A fatal error has occurred: {}", message);
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {
    LOG.info("Received a framework message");
  }

  @Override
  public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
    LOG.info("Killing task {}", taskId.getValue());
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

          LOG.info("Launching task {}", task.getTaskId().getValue());

          Thread.currentThread().setContextClassLoader(
              UnderFileSystemFactoryRegistry.class.getClassLoader());

          Format.format(Format.Mode.MASTER);
          AlluxioMaster.main(new String[] {});

          status =
              Protos.TaskStatus.newBuilder().setTaskId(task.getTaskId())
                  .setState(Protos.TaskState.TASK_FINISHED).build();

          driver.sendStatusUpdate(status);
        } catch (Exception e) {
          LOG.error("Error starting Alluxio master", e);
        }
      }
    }.start();
  }

  @Override
  public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo,
      Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
    LOG.info("Registered executor {} with {} through framework {}",
        executorInfo.getName(), slaveInfo.getHostname(), frameworkInfo.getName());
  }

  @Override
  public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
    LOG.info("Re-registered executor with {}", slaveInfo.getHostname());
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    LOG.info("Shutting down");
    // TODO(jiri): Implement.
  }

  /**
   * Starts the Alluxio master executor.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) throws Exception {
    MesosExecutorDriver driver = new MesosExecutorDriver(new AlluxioMasterExecutor());
    System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
  }
}
