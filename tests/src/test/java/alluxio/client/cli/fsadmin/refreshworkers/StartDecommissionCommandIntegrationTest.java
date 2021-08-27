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

package alluxio.client.cli.fsadmin.refreshworkers;

import static org.junit.Assert.assertEquals;

import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;

import org.junit.Test;

import java.io.IOException;

/**
 * Integration tests for the refresh workers sub command.
 */
public final  class StartDecommissionCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void starDecommission() throws IOException {
    int errCode = mFsAdminShell.run("refreshWorkers", "start"
        ,"--excluded-worker", "host1");
    assertEquals("", mErrOutput.toString());
    assertEquals(0, errCode);
    errCode = mFsAdminShell.run("refreshWorkers", "start");
    assertEquals("", mErrOutput.toString());
    assertEquals(0, errCode);
    errCode = mFsAdminShell.run("refreshWorkers", "start", "--exclude-hosts"
    , "host1", "host2");
    assertEquals("", mErrOutput.toString());
    assertEquals(0, errCode);
  }
}
