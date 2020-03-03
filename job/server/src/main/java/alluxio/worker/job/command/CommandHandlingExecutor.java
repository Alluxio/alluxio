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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.grpc.CancelTaskCommand;
import alluxio.grpc.JobCommand;
import alluxio.grpc.JobInfo;
import alluxio.grpc.RunTaskCommand;
import alluxio.grpc.SetTaskPoolSizeCommand;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.job.JobServerContext;
import alluxio.job.RunTaskContext;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.TaskInfo;
import alluxio.worker.job.JobMasterClient;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.JobWorkerIdRegistry;
import alluxio.worker.job.task.TaskExecutorManager;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Manages the communication with the master. Dispatches the received command from master to
 * handlers, and sends the status of all the tasks to master.
 */
@NotThreadSafe
public class CommandHandlingExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CommandHandlingExecutor.class);

  private final JobServerContext mServerContext;
  private final JobMasterClient mMasterClient;
  private final TaskExecutorManager mTaskExecutorManager;
  private final WorkerNetAddress mWorkerNetAddress;
  private final JobWorkerHealthReporter mHealthReporter;

  // Keep this single threaded to keep the order of command execution consistent
  private final ExecutorService mCommandHandlingService =
      Executors.newSingleThreadExecutor(
          ThreadFactoryUtils.build("command-handling-service", true));

  /**
   * Creates a new instance of {@link CommandHandlingExecutor}.
   *
   * @param jobServerContext the job worker's context used to execute tasks
   * @param taskExecutorManager the {@link TaskExecutorManager}
   * @param masterClient the {@link JobMasterClient}
   * @param workerNetAddress the connection info for this worker
   */
  public CommandHandlingExecutor(JobServerContext jobServerContext,
      TaskExecutorManager taskExecutorManager, JobMasterClient masterClient,
      WorkerNetAddress workerNetAddress) {
    mServerContext = Preconditions.checkNotNull(jobServerContext);
    mTaskExecutorManager = Preconditions.checkNotNull(taskExecutorManager, "taskExecutorManager");
    mMasterClient = Preconditions.checkNotNull(masterClient, "masterClient");
    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress, "workerNetAddress");
    if (ServerConfiguration.getBoolean(PropertyKey.JOB_WORKER_THROTTLING)) {
      mHealthReporter = new JobWorkerHealthReporter();
    } else {
      mHealthReporter = new AlwaysHealthyJobWorkerHealthReporter();
    }
  }

  @Override
  public void heartbeat() {
    mHealthReporter.compute();

    if (mHealthReporter.isHealthy()) {
      mTaskExecutorManager.unthrottle();
    } else {
      mTaskExecutorManager.throttle();
    }

    JobWorkerHealth jobWorkerHealth = new JobWorkerHealth(JobWorkerIdRegistry.getWorkerId(),
        mHealthReporter.getCpuLoadAverage(), mTaskExecutorManager.getTaskExecutorPoolSize(),
        mTaskExecutorManager.getNumActiveTasks(), mTaskExecutorManager.unfinishedTasks(),
        mWorkerNetAddress.getHost());

    List<TaskInfo> taskStatusList = mTaskExecutorManager.getAndClearTaskUpdates();

    List<alluxio.grpc.JobCommand> commands;

    List<JobInfo> taskProtoList = taskStatusList.stream().map(TaskInfo::toProto)
        .collect(Collectors.toList());

    try {
      commands = mMasterClient.heartbeat(jobWorkerHealth, taskProtoList);
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
        long taskId = command.getTaskId();
        RunTaskContext context = new RunTaskContext(jobId, taskId, mServerContext);
        LOG.info("Received run task " + taskId + " for job " + jobId + " on worker "
            + JobWorkerIdRegistry.getWorkerId());
        mTaskExecutorManager.executeTask(jobId, taskId, command, context);
      } else if (mCommand.hasCancelTaskCommand()) {
        CancelTaskCommand command = mCommand.getCancelTaskCommand();
        long jobId = command.getJobId();
        long taskId = command.getTaskId();
        mTaskExecutorManager.cancelTask(jobId, taskId);
      } else if (mCommand.hasRegisterCommand()) {
        try {
          JobWorkerIdRegistry.registerWorker(mMasterClient, mWorkerNetAddress);
        } catch (ConnectionFailedException | IOException e) {
          Throwables.throwIfUnchecked(e);
          throw new RuntimeException(e);
        }
      } else if (mCommand.hasSetTaskPoolSizeCommand()) {
        SetTaskPoolSizeCommand command = mCommand.getSetTaskPoolSizeCommand();
        LOG.info(String.format("Task Pool Size: %s", command.getTaskPoolSize()));
        mTaskExecutorManager.setDefaultTaskExecutorPoolSize(command.getTaskPoolSize());
      } else {
        throw new RuntimeException("unsupported command type:" + mCommand.toString());
      }
    }
  }
}
