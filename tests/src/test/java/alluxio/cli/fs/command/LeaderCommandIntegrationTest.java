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

package alluxio.cli.fs.command;

import alluxio.cli.fs.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for leader command.
 */
public final class LeaderCommandIntegrationTest extends AbstractAlluxioShellTest {

  @Test
  public void leader() {
    mFsShell.run("leader");
    String expected =
        mLocalAlluxioCluster.getLocalAlluxioMaster().getAddress().getHostName() + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void leaderAddressNotAvailable() throws Exception {
    mLocalAlluxioCluster.stopMasters();
    mFsShell.run("leader");
    String expected =
            mLocalAlluxioCluster.getLocalAlluxioMaster().getAddress().getHostName() + "\n" +
            "Leader master RPC address is not available." + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
