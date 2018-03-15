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

package alluxio.cli.fsadmin.command;

import alluxio.cli.fsadmin.AbstractFsadminShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for report command.
 */
public final class ReportCommandIntegrationTest extends AbstractFsadminShellTest {
  @Test
  public void masterNotRunning() throws Exception {
    mLocalAlluxioCluster.stopMasters();
    int ret = mFsAdminShell.run("report");
    Assert.assertEquals(1, ret);
    String expected = "The Alluxio leader master is not currently serving requests.\n"
        + "Please check your Alluxio master status\n";
    Assert.assertEquals(expected, mErrOutput.toString());
  }

  @Test
  public void reportCategoryInvalid() {
    int ret = mFsAdminShell.run("report", "invalidCategory");
    Assert.assertEquals(1, ret);
    ReportCommand reportCommand = new ReportCommand();
    String expected = reportCommand.getUsage() + "\n" + reportCommand.getDescription() + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
