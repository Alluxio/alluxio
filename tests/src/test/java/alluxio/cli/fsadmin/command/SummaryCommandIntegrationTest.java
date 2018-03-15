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
import alluxio.cli.fsadmin.report.SummaryCommand;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.MetaMasterClient;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.Configuration;
import alluxio.master.MasterClientConfig;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

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

  @Test
  public void callSummaryWithDependencies() throws IOException {
    try (MetaMasterClient mMetaMasterClient
        = new RetryHandlingMetaMasterClient(MasterClientConfig.defaults());
        RetryHandlingBlockMasterClient mBlockMasterClient = new RetryHandlingBlockMasterClient(
        MasterClientConfig.defaults())) {
      SummaryCommand summaryCommand = new SummaryCommand(mMetaMasterClient, mBlockMasterClient);
      summaryCommand.run();
      String output = mOutput.toString();
      checkSummaryResults(output);
    }
  }

  /**
   * Checks if the summary command output is valid.
   *
   * @param output the summary command output
   */
  private void checkSummaryResults(String output) {
    // Checks if meta master values are available
    String expectedMasterAddress = NetworkAddressUtils
        .getConnectAddress(ServiceType.MASTER_RPC).toString();
    Assert.assertTrue(output.contains("Master Address: " + expectedMasterAddress));
    Assert.assertTrue(output.contains("Web Port: "
        + Configuration.get(PropertyKey.MASTER_WEB_PORT)));
    Assert.assertTrue(output.contains("Rpc Port: "
        + Configuration.get(PropertyKey.MASTER_RPC_PORT)));
    Assert.assertTrue(!output.contains("Started: 12-31-1969 16:00:00:000"));
    Assert.assertTrue(output.contains("Version: " + ProjectConstants.VERSION));

    // Checks if block master values are available
    Assert.assertTrue(!output.contains("Live Workers: 0")
        || !output.contains("Lost Workers: 0"));
    Assert.assertTrue(!output.contains("Total Capacity: 0B"));
  }
}
