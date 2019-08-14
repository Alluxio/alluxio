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

import alluxio.cli.fsadmin.command.DoctorCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for doctor command.
 */
public final class DoctorCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void masterNotRunning() throws Exception {
    mLocalAlluxioCluster.stopMasters();
    int ret = mFsAdminShell.run("doctor");
    Assert.assertNotEquals(0, ret);
  }

  @Test
  public void doctorCategoryInvalid() {
    mFsAdminShell.run("doctor", "invalidCategory");
    String expected = String.join("\n",
        DoctorCommand.usage(),
        DoctorCommand.description(),
        "doctor category is invalid.") + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void doctorConfiguration() {
    int ret = mFsAdminShell.run("doctor", "configuration");
    Assert.assertEquals(0, ret);
    String expected = "No server-side configuration errors or warnings.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void doctorStorage() {
    int ret = mFsAdminShell.run("doctor", "storage");
    Assert.assertEquals(0, ret);
    String expected = "All worker storage paths are in working state.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
