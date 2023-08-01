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

package alluxio.client.cli.job;

import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.job.SleepJobConfig;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.Status;

import org.junit.Ignore;
import org.junit.Test;

@DoraTestTodoItem(action = DoraTestTodoItem.Action.REMOVE, owner = "Jianjian",
    comment = "Job master and job worker no longer exists in dora")
@Ignore
public class CancelCommandTest extends JobShellTest {

  @Test
  public void testCancel() throws Exception {
    long jobId = sJobMaster.run(new SleepJobConfig(150 * 1000));

    sJobShell.run("cancel", Long.toString(jobId));

    JobTestUtils.waitForJobStatus(sJobMaster, jobId, Status.CANCELED);
  }
}
