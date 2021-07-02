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

package alluxio.master.job.command;

import alluxio.grpc.CancelTaskCommand;
import alluxio.grpc.JobCommand;
import alluxio.grpc.RunTaskCommand;
import alluxio.grpc.SetTaskPoolSizeCommand;
import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A command manager that manages the commands to issue to the workers.
 */
@ThreadSafe
public final class CommandManager {

  private static final Logger LOG = LoggerFactory.getLogger(CommandManager.class);

  // TODO(yupeng) add retry support
  private final Map<Long, List<JobCommand>> mWorkerIdToPendingCommands = Maps.newHashMap();

  /**
   * Constructs a new {@link CommandManager}.
   */
  public CommandManager() {}

  /**
   * Submits a run-task command to a specified worker.
   *
   * @param jobId the id of the job
   * @param taskId the id of the task
   * @param jobConfig the job configuration
   * @param taskArgs the arguments passed to the executor on the worker
   * @param workerId the id of the worker
   */
  public synchronized void submitRunTaskCommand(long jobId, long taskId, JobConfig jobConfig,
      Object taskArgs, long workerId) {
    RunTaskCommand.Builder runTaskCommand = RunTaskCommand.newBuilder();
    runTaskCommand.setJobId(jobId);
    runTaskCommand.setTaskId(taskId);
    try {
      runTaskCommand.setJobConfig(ByteString.copyFrom(SerializationUtils.serialize(jobConfig)));
      if (taskArgs != null) {
        runTaskCommand.setTaskArgs(ByteString.copyFrom(SerializationUtils.serialize(taskArgs)));
      }
    } catch (IOException e) {
      // TODO(yupeng) better exception handling
      LOG.info("Failed to serialize the run task command:" + e);
      return;
    }
    JobCommand.Builder command = JobCommand.newBuilder();
    command.setRunTaskCommand(runTaskCommand);
    submit(workerId, command);
  }

  /**
   * Submits a cancel-task command to a specified worker.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param workerId the worker id
   */
  public synchronized void submitCancelTaskCommand(long jobId, long taskId, long workerId) {
    CancelTaskCommand.Builder cancelTaskCommand = CancelTaskCommand.newBuilder();
    cancelTaskCommand.setJobId(jobId);
    cancelTaskCommand.setTaskId(taskId);
    JobCommand.Builder command = JobCommand.newBuilder();
    command.setCancelTaskCommand(cancelTaskCommand);
    submit(workerId, command);
  }

  /**
   * Submits a set thread pool size command to specific worker.
   *
   * @param workerId the worker id
   * @param taskPoolSize the task pool size
   */
  public synchronized void submitSetTaskPoolSizeCommand(long workerId, int taskPoolSize) {
    SetTaskPoolSizeCommand.Builder setTaskPoolSizeCommand = SetTaskPoolSizeCommand.newBuilder();
    setTaskPoolSizeCommand.setTaskPoolSize(taskPoolSize);

    JobCommand.Builder command = JobCommand.newBuilder();
    command.setSetTaskPoolSizeCommand(setTaskPoolSizeCommand);
    submit(workerId, command);
  }

  private synchronized void submit(long workerId, JobCommand.Builder command) {
    if (!mWorkerIdToPendingCommands.containsKey(workerId)) {
      mWorkerIdToPendingCommands.put(workerId, Lists.newArrayList());
    }
    mWorkerIdToPendingCommands.get(workerId).add(command.build());
  }

  /**
   * Polls all the pending commands to a worker and removes the commands from the queue.
   *
   * @param workerId id of the worker to send the commands to
   * @return the list of the commends polled
   */
  public synchronized List<alluxio.grpc.JobCommand> pollAllPendingCommands(long workerId) {
    if (!mWorkerIdToPendingCommands.containsKey(workerId)) {
      return Lists.newArrayList();
    }
    List<JobCommand> commands =
        Lists.newArrayList(mWorkerIdToPendingCommands.get(workerId));
    mWorkerIdToPendingCommands.get(workerId).clear();
    return commands;
  }
}
