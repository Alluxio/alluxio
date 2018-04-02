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

import alluxio.cli.fsadmin.AbstractFsAdminShellTest;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for report operation command.
 */
public final class OperationCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void runOperationShell() {
    int ret = mFsAdminShell.run("report", "operation");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();
    checkOperationResults(output);
  }

  /**
   * Checks if the operation command output is valid.
   *
   * @param output the report operation command output
   */
  private void checkOperationResults(String output) {
    Assert.assertThat(output, CoreMatchers.containsString("Alluxio logical operations: "));
    Assert.assertThat(output, CoreMatchers.containsString(
        "    Paths Deleted                           0\n"
        + "    Paths Mounted                           0\n"
        + "    Paths Renamed                           0\n"
        + "    Paths Unmounted                         0"));
    Assert.assertFalse(output.contains("UfsSessionCount-Ufs"));
    Assert.assertThat(output, CoreMatchers.containsString("Alluxio RPC invocations: \n"
        + "    Complete File Operations                0\n"
        + "    Create Directory Operations             0\n"
        + "    Create File Operations                  0\n"
        + "    Delete Path Operations                  0"));
  }
}
