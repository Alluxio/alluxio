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
import alluxio.Configuration;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

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
    Assert.assertThat(output, CoreMatchers.containsString("asfdasda"));
  }
}
