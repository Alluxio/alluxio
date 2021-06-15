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

import alluxio.cli.fsadmin.command.UpdateConfCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for updateConf command.
 */
public final class UpdateConfIntegrationTest extends AbstractFsAdminShellTest {

  @Test
  public void unknownOption() {
    int ret = mFsAdminShell.run("updateConf", "--unknown");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(UpdateConfCommand.description(), lastLine(output));
  }

  @Test
  public void updateUnknownKey() {
    int ret = mFsAdminShell.run("updateConf", "unknown-key=unknown-value");
    Assert.assertEquals(-2, ret);
    ret = mFsAdminShell.run("updateConf", "unknown-key");
    Assert.assertEquals(-1, ret);
    ret = mFsAdminShell.run("updateConf", "unknown-key=1=2");
    Assert.assertEquals(-3, ret);
  }

  @Test
  public void updateNormal() {
    int ret = mFsAdminShell.run("updateConf", "alluxio.master.worker.timeout=4min");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void updateNonDynamicKey() {
    int ret = mFsAdminShell.run("updateConf",
        "alluxio.security.authorization.permission.enabled=false");
    Assert.assertEquals(-2, ret);
  }

  private String lastLine(String output) {
    String[] lines = output.split("\n");
    if (lines.length > 0) {
      return lines[lines.length - 1];
    }
    return "";
  }
}
