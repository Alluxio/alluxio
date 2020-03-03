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

import alluxio.grpc.JobCommand;
import alluxio.job.JobConfig;
import alluxio.job.TestPlanConfig;
import alluxio.job.util.SerializationUtils;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link CommandManager}.
 */
public final class CommandManagerTest {
  private CommandManager mManager;

  @Before
  public void before() {
    mManager = new CommandManager();
  }

  @Test
  public void submitRunTaskCommand() throws Exception {
    long jobId = 0L;
    int taskId = 1;
    JobConfig jobConfig = new TestPlanConfig("/test");
    long workerId = 2L;
    List<Integer> args = Lists.newArrayList(1);
    mManager.submitRunTaskCommand(jobId, taskId, jobConfig, args, workerId);
    List<JobCommand> commands = mManager.pollAllPendingCommands(workerId);
    Assert.assertEquals(1, commands.size());
    JobCommand command = commands.get(0);
    Assert.assertEquals(jobId, command.getRunTaskCommand().getJobId());
    Assert.assertEquals(taskId, command.getRunTaskCommand().getTaskId());
    Assert.assertEquals(jobConfig,
        SerializationUtils.deserialize(command.getRunTaskCommand().getJobConfig().toByteArray()));
    Assert.assertEquals(args,
        SerializationUtils.deserialize(command.getRunTaskCommand().getTaskArgs().toByteArray()));
  }

  @Test
  public void submitCancelTaskCommand() {
    long jobId = 0L;
    int taskId = 1;
    long workerId = 2L;
    mManager.submitCancelTaskCommand(jobId, taskId, workerId);
    List<JobCommand> commands = mManager.pollAllPendingCommands(workerId);
    Assert.assertEquals(1, commands.size());
    JobCommand command = commands.get(0);
    Assert.assertEquals(jobId, command.getCancelTaskCommand().getJobId());
    Assert.assertEquals(taskId, command.getCancelTaskCommand().getTaskId());
  }
}
