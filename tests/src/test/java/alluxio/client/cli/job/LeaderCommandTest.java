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

package alluxio.client.cli.job;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the job leader shell command.
 */
public final class LeaderCommandTest extends JobShellTest {
  @Test
  public void jobLeader() {
    sJobShell.run("leader");
    String expected =
        sLocalAlluxioCluster.getLocalAlluxioMaster().getAddress().getHostName() + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
