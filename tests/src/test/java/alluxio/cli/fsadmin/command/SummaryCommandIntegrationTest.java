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
import alluxio.Configuration;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for report summary command.
 */
public final class SummaryCommandIntegrationTest extends AbstractFsadminShellTest {
  @Test
  public void runSummaryShell() {
    int ret = mFsAdminShell.run("report", "summary");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();
    checkSummaryResults(output);
  }

  /**
   * Checks if the summary command output is valid.
   *
   * @param output the summary command output
   */
  private void checkSummaryResults(String output) {
    // Check if meta master values are available
    String expectedMasterAddress = NetworkAddressUtils
        .getConnectAddress(ServiceType.MASTER_RPC).toString();
    Assert.assertThat(output, CoreMatchers.containsString(
        "Master Address: " + expectedMasterAddress));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Web Port: " + Configuration.get(PropertyKey.MASTER_WEB_PORT)));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Rpc Port: " + Configuration.get(PropertyKey.MASTER_RPC_PORT)));
    Assert.assertFalse(output.contains("Started: 12-31-1969 16:00:00:000"));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Version: " + ProjectConstants.VERSION));

    // Check if block master values are available
    Assert.assertThat(output, CoreMatchers.containsString("Live Workers: 1"));
    Assert.assertFalse(output.contains("Total Capacity: 0B"));
  }
}
