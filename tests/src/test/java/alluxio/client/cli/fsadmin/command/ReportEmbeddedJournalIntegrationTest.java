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

import alluxio.ProjectConstants;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.network.NetworkAddressUtils;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

@LocalAlluxioClusterResource.ServerConfig(
    confParams = {
        PropertyKey.Name.MASTER_JOURNAL_TYPE, "EMBEDDED",
        PropertyKey.Name.MASTER_EMBEDDED_JOURNAL_PORT,
        ReportEmbeddedJournalIntegrationTest.JOURNAL_PORT})
public class ReportEmbeddedJournalIntegrationTest  extends AbstractFsAdminShellTest {
  static final String JOURNAL_PORT = "61234";

  @Test
  public void reportSummary() {
    int ret = mFsAdminShell.run("report", "summary");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();

    // Check if meta master values are available
    String expectedMasterAddress = NetworkAddressUtils
        .getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC,
            Configuration.global()).toString();
    Assert.assertThat(output, CoreMatchers.containsString(
        "Master Address: " + expectedMasterAddress));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Web Port: " + Configuration.get(PropertyKey.MASTER_WEB_PORT)));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Rpc Port: " + Configuration.get(PropertyKey.MASTER_RPC_PORT)));
    Assert.assertFalse(output.contains("Started: 12-31-1969 16:00:00:000"));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Version: " + ProjectConstants.VERSION));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Zookeeper Enabled: false"));
    Assert.assertThat(output, CoreMatchers.containsString("Raft-based Journal: true"));
    Assert.assertThat(output, CoreMatchers.containsString("Raft Journal Addresses:"));
    Assert.assertThat(output, CoreMatchers.containsString(
        mLocalAlluxioCluster.getHostname() + ":" + JOURNAL_PORT));

    // Check if block master values are available
    Assert.assertThat(output, CoreMatchers.containsString("Live Workers: 1"));
    Assert.assertFalse(output.contains("Total Capacity: 0B"));
  }
}
