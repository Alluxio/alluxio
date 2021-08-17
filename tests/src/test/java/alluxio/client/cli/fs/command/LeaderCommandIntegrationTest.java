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

package alluxio.client.cli.fs.command;

import alluxio.client.cli.fs.AbstractFileSystemShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for leader command.
 */
public final class LeaderCommandIntegrationTest extends AbstractFileSystemShellTest {

  @Test
  public void leader() {
    sFsShell.run("leader");
    String expected = "This command will be deprecated as of v3.0, please use masterInfo command\n"
        + sLocalAlluxioCluster.getLocalAlluxioMaster().getAddress().getHostName() + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void leaderAddressNotAvailable() throws Exception {
    sLocalAlluxioCluster.stopMasters();
    sFsShell.run("leader");
    String expected = "The leader is not currently serving requests.";
    Assert.assertTrue(mErrOutput.toString().contains(expected));
  }
}
