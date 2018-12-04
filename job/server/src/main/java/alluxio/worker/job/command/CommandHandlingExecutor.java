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

package alluxio.worker.job.command;

import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.grpc.CancelTaskCommand;
import alluxio.grpc.JobCommand;
import alluxio.grpc.RunTaskCommand;
import alluxio.grpc.TaskInfo;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.job.JobConfig;
import alluxio.job.JobWorkerContext;
import alluxio.underfs.UfsManager;
import alluxio.worker.job.JobMasterClient;
import alluxio.job.util.SerializationUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.JobWorkerIdRegistry;
import alluxio.worker.job.task.TaskExecutorManager;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Manages the communication with the master. Dispatches the received command from master to
 * handlers, and sends the status of all the tasks to master.
 */
@NotThreadSafe
public class CommandHandlingExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CommandHandlingExecutor.class);
  private static final int DEFAULT_COMMAND_HANDLING_POOL_SIZE = 4;

  private final JobMasterClient mMasterClient;
  private final TaskExecutorManager mTaskExecutorManager;
  private final WorkerNetAddress mWorkerNetAddress;

  private final ExecutorService mCommandHandlingService =
      Executors.newFixedThreadPool(DEFAULT_COMMAND_HANDLING_POOL_SIZE,
          ThreadFactoryUtils.build("command-handling-service-%d", true));
  /** The manager for all ufs. */
  private UfsManager mUfsManager;

  /**
   * Creates a new instance of {@link CommandHandlingExecutor}.
   *
   * @param taskExecutorManager the {@link TaskExecutorManager}
   * @param ufsManager the {@link UfsManager}
   * @param masterClient the {@link JobMasterClient}
   * @param workerNetAddress the connection info for this worker
   */
  public CommandHandlingExecutor(TaskExecutorManager taskExecutorManager, UfsManager ufsManager,
      JobMasterClient masterClient, WorkerNetAddress workerNetAddress) {
    mTaskExecutorManager = Preconditions.checkNotNull(taskExecutorManager, "taskExecutorManager");
    mUfsManager = Preconditions.checkNotNull(ufsManager, "ufsManager");
    mMasterClient = Preconditions.checkNotNull(masterClient, "masterClient");
    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress, "workerNetAddress");
  }

  @Override
  public void heartbeat() {
    List<TaskInfo> taskStatusList = mTaskExecutorManager.getAndClearTaskUpdates();

    List<alluxio.grpc.JobCommand> commands;
    try {
      commands = mMasterClient.heartbeat(JobWorkerIdRegistry.getWorkerId(), taskStatusList);
    } catch (AlluxioException | IOException e) {
      // Restore the task updates so that they can be accessed in the next heartbeat.
      mTaskExecutorManager.restoreTaskUpdates(taskStatusList);
      // TODO(yupeng) better error handling
      LOG.error("Failed to heartbeat", e);
      return;
    }

    for (JobCommand command : commands) {
      mCommandHandlingService.execute(new CommandHandler(command));
    }
  }

  @Override
  public void close() {}

  /**
   * A handler that handles a command sent from the master.
   */
  class CommandHandler implements Runnable {
    private final JobCommand mCommand;

    CommandHandler(JobCommand command) {
      mCommand = command;
    }

    @Override
    public void run() {
      if (mCommand.hasRunTaskCommand()) {
        RunTaskCommand command = mCommand.getRunTaskCommand();
        long jobId = command.getJobId();
        int taskId = command.getTaskId();
        JobConfig jobConfig;
        try {
          jobConfig =
              (JobConfig) SerializationUtils.deserialize(command.getJobConfig().toByteArray());
          Serializable taskArgs = null;
          if(command.hasTaskArgs()) {
            taskArgs = SerializationUtils.deserialize(command.getTaskArgs().toByteArray());
          }
          JobWorkerContext context = new JobWorkerContext(jobId, taskId, mUfsManager);
          LOG.info("Received run task " + taskId + " for job " + jobId + " on worker "
              + JobWorkerIdRegistry.getWorkerId());
          mTaskExecutorManager.executeTask(jobId, taskId, jobConfig, taskArgs, context);
        } catch (ClassNotFoundException | IOException e) {
          // TODO(yupeng) better error handling
          LOG.error("Failed to deserialize ", e);
        }
      } else if (mCommand.hasCancelTaskCommand()) {
        CancelTaskCommand command = mCommand.getCancelTaskCommand();
        long jobId = command.getJobId();
        int taskId = command.getTaskId();
        mTaskExecutorManager.cancelTask(jobId, taskId);
      } else if (mCommand.hasRegisterCommand()) {
        try {
          JobWorkerIdRegistry.registerWorker(mMasterClient, mWorkerNetAddress);
        } catch (ConnectionFailedException | IOException e) {
          throw Throwables.propagate(e);
        }
      } else {
        throw new RuntimeException("unsupported command type:" + mCommand.toString());
      }
    }
  }
}
