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

import alluxio.Configuration;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.cli.fsadmin.command.ReportCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.util.network.NetworkAddressUtils;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests for report command.
 */
public final class ReportCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void masterNotRunning() throws Exception {
    mLocalAlluxioCluster.stopMasters();
    mFsAdminShell.run("report");
    String expected = "The Alluxio leader master is not currently serving requests, "
        + "please check your Alluxio master status.";
    Assert.assertThat(mOutput.toString(), CoreMatchers.containsString(expected));
  }

  @Test
  public void reportCategoryInvalid() {
    mFsAdminShell.run("report", "invalidCategory");
    ReportCommand reportCommand = new ReportCommand();
    String expected = String.format("%s%n%s%n%s%n",
        reportCommand.getUsage(),
        reportCommand.getDescription(),
        "report category is invalid.");
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void reportConfiguration() {
    int ret = mFsAdminShell.run("report", "configuration");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();
    Assert.assertThat(output,
        CoreMatchers.containsString("Alluxio configuration information:"));

    // Output should not contain raw values with ${VALUE} format
    String regexString = "(\\$\\{([^{}]*)\\})";
    Pattern confRegex = Pattern.compile(regexString);
    Matcher matcher = confRegex.matcher(output);
    Assert.assertFalse(matcher.find());

    // Output should contain all kinds of properties.
    Assert.assertTrue(output.contains("alluxio.debug"));
    Assert.assertTrue(output.contains("alluxio.fuse.fs.name"));
    Assert.assertTrue(output.contains("alluxio.logserver.logs.dir"));
    Assert.assertTrue(output.contains("alluxio.master.journal.folder"));
    Assert.assertTrue(output.contains("alluxio.proxy.web.port"));
    Assert.assertTrue(output.contains("alluxio.security.authentication.type"));
    Assert.assertTrue(output.contains("alluxio.user.block.master.client.threads"));
    Assert.assertTrue(output.contains("alluxio.worker.bind.host"));
  }

  @Test
  public void reportSummary() {
    int ret = mFsAdminShell.run("report", "summary");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();

    // Check if meta master values are available
    String expectedMasterAddress = NetworkAddressUtils
        .getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC).toString();
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
        CoreMatchers.containsString("Alluxio under filesystem information: "));
    Assert.assertThat(output,
        CoreMatchers.containsString("not read-only, not shared, properties={})"));
  }
}
