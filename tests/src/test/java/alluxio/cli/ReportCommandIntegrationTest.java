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

package alluxio.cli;

import alluxio.BaseIntegrationTest;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.SystemErrRule;
import alluxio.SystemOutRule;
import alluxio.cli.fsadmin.FileSystemAdminShell;
import alluxio.cli.fsadmin.command.ReportCommand;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;

public class ReportCommandIntegrationTest extends BaseIntegrationTest{
  private static final int SIZE_BYTES = Constants.MB * 16;
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, SIZE_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, SIZE_BYTES)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Integer.MAX_VALUE).build();
  private LocalAlluxioCluster mLocalAlluxioCluster = null;
  private FileSystemAdminShell mFsAdminShell = null;
  private ByteArrayOutputStream mOutput = new ByteArrayOutputStream();
  private ByteArrayOutputStream mErrOutput = new ByteArrayOutputStream();

  @Rule
  public ExpectedException mException = ExpectedException.none();
  @Rule
  public SystemOutRule mOutRule = new SystemOutRule(mOutput);
  @Rule
  public SystemErrRule mErrRule = new SystemErrRule(mErrOutput);

  @Before
  public final void before() throws Exception {
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mFsAdminShell = new FileSystemAdminShell();
  }

  @After
  public final void after() throws Exception {
    mFsAdminShell.close();
  }

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
    Assert.assertEquals(-1, ret);
    ReportCommand reportCommand = new ReportCommand();
    String expected = reportCommand.getUsage() + "\n" + reportCommand.getDescription() + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void reportSummaryInfoCheck() {
    int ret = mFsAdminShell.run("report", "summary");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();

    // Checks if meta master values are available
    String expectedMasterAddress = NetworkAddressUtils
        .getConnectAddress(ServiceType.MASTER_RPC).toString();
    Assert.assertTrue(output.contains("Master Address: " + expectedMasterAddress));
    Assert.assertTrue(output.contains("Web Port: "
        + Configuration.get(PropertyKey.MASTER_WEB_PORT)));
    Assert.assertTrue(output.contains("Rpc Port: "
        + Configuration.get(PropertyKey.MASTER_RPC_PORT)));
    Assert.assertFalse(output.contains("Started: 12-31-1969 16:00:00:000"));
    Assert.assertTrue(output.contains("Version: " + ProjectConstants.VERSION));

    // Checks if block master values are available
    Assert.assertTrue(!output.contains("Live Workers: 0")
        || !output.contains("Lost Workers: 0"));
    Assert.assertTrue(!output.contains("Total Capacity: 0B"));
  }
}
