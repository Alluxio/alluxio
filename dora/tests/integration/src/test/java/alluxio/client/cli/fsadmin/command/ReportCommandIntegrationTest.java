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

package alluxio.client.cli.fsadmin.command;

import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.cli.fsadmin.command.ReportCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for report command.
 */
public final class ReportCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  @DoraTestTodoItem(owner = "jiacheng", action = DoraTestTodoItem.Action.FIX,
      comment = "need to check if this command will still exist in Dora")
  @Ignore
  public void masterNotRunning() throws Exception {
    mLocalAlluxioCluster.stopMasters();
    int ret = mFsAdminShell.run("report");
    Assert.assertNotEquals(0, ret);
  }

  @Test
  public void reportCategoryInvalid() {
    mFsAdminShell.run("report", "invalidCategory");
    String expected = String.join("\n",
        ReportCommand.usage(),
        ReportCommand.description(),
        "report category is invalid.") + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Ignore
  @DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "lu",
      comment = "Bring back but not passed, need to fix.")
  @Test
  public void reportUfs() {
    int ret = mFsAdminShell.run("report", "ufs");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();
    Assert.assertThat(output,
        CoreMatchers.containsString("Alluxio under storage system information:"));
    Assert.assertThat(output,
        CoreMatchers.containsString("not read-only, not shared, properties={})"));
  }
}
