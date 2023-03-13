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

import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MultiMasterEmbeddedJournalLocalAlluxioCluster;
import alluxio.multi.process.PortCoordination;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * Tests for checkpoint command.
 */
public final class CheckpointCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void checkpoint() throws Exception {
    int ret = mFsAdminShell.run("journal", "checkpoint");
    Assert.assertEquals(0, ret);
    Assert.assertThat(mOutput.toString(), CoreMatchers.containsString(String
        .format("Successfully took a checkpoint on master %s%n",
        sLocalAlluxioClusterResource.get().getHostname())));
  }

  @Test
  public void targetedCheckpoint() throws Exception {
    Configuration.set(PropertyKey.STANDBY_MASTER_GRPC_ENABLED, true);
    MultiMasterEmbeddedJournalLocalAlluxioCluster cluster =
        new MultiMasterEmbeddedJournalLocalAlluxioCluster(3, 1,
            PortCoordination.CHECKPOINT_SHELL);
    try {
      cluster.initConfiguration("targetedCheckpoint");
      cluster.start();
      cluster.waitForPrimaryMasterServing(5_000);
      for (InetSocketAddress address : cluster.getMasterAddresses()) {
        String strAddr = String.format("%s:%d", address.getHostName(), address.getPort());
        int ret = mFsAdminShell.run("journal", "checkpoint", "-address", strAddr);
        Assert.assertEquals(0, ret);
        Assert.assertTrue(mOutput.toString()
            .contains(String.format("Successfully took a checkpoint on master %s%n", strAddr)));
      }
    } catch (Exception e) {
      System.out.println(e);
    } finally {
      cluster.stop();
    }
  }
}
