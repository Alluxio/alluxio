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

import alluxio.conf.ServerConfiguration;
import alluxio.ProjectConstants;
import alluxio.conf.PropertyKey;
import alluxio.cli.fsadmin.command.ReportCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.util.network.NetworkAddressUtils;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for report command.
 */
public final class ReportCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
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

  @Test
  public void reportSummary() {
    int ret = mFsAdminShell.run("report", "summary");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();

    // Check if meta master values are available
    String expectedMasterAddress = NetworkAddressUtils
        .getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC,
            ServerConfiguration.global()).toString();
    Assert.assertThat(output, CoreMatchers.containsString(
        "Master Address: " + expectedMasterAddress));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Web Port: " + ServerConfiguration.get(PropertyKey.MASTER_WEB_PORT)));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Rpc Port: " + ServerConfiguration.get(PropertyKey.MASTER_RPC_PORT)));
    Assert.assertFalse(output.contains("Started: 12-31-1969 16:00:00:000"));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Version: " + ProjectConstants.VERSION));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Zookeeper Enabled: false"));

    // Check if block master values are available
    Assert.assertThat(output, CoreMatchers.containsString("Live Workers: 1"));
    Assert.assertFalse(output.contains("Total Capacity: 0B"));
  }

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
