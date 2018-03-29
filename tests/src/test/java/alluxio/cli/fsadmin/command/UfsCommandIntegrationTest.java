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

package alluxio.cli.fsadmin.command;

import alluxio.cli.fsadmin.AbstractFsAdminShellTest;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for report ufs command.
 */
public final class UfsCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void ufs() {
    mFsAdminShell.run("report", "ufs");
    Assert.assertThat(mOutput.toString(),
        CoreMatchers.containsString("Alluxio configuration information:"));

  }
}
