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
import alluxio.cli.fsadmin.report.CapacityCommand;
import alluxio.util.FormatUtils;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for report capacity command.
 */
public final class CapacityCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void allCapacity() {
    int ret = mFsAdminShell.run("report", "capacity");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();
    String size = FormatUtils.getSizeFromBytes(SIZE_BYTES);
    Assert.assertThat(output, CoreMatchers.containsString("Capacity information for all workers: \n"
        + "    Total Capacity: " + size + "\n"
        + "        Tier: MEM  Size: " + size + "\n"
        + "    Used Capacity: 0B\n"
        + "        Tier: MEM  Size: 0B\n"
        + "    Used Percentage: 0%\n"
        + "    Free Percentage: 100%\n"));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Worker Name       Type          Total            MEM           SSD           HDD"));
    Assert.assertThat(output, CoreMatchers.containsString(
        "                  Used          0B (0%)          0B            0B            0B"));
  }

  @Test
  public void lostCapacity() {
    int ret = mFsAdminShell.run("report", "capacity", "-lost");
    Assert.assertEquals(0, ret);
    Assert.assertThat(mOutput.toString(), CoreMatchers.containsString(
        "Capacity information for lost workers: \n"
        + "    Total Capacity: 0B\n"
        + "    Used Capacity: 0B\n"));
  }

  @Test
  public void liveCapacity() {
    int ret = mFsAdminShell.run("report", "capacity", "-live");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();
    String size = FormatUtils.getSizeFromBytes(SIZE_BYTES);
    Assert.assertThat(output, CoreMatchers.containsString(
        "Capacity information for live workers: \n"
        + "    Total Capacity: " + size + "\n"
        + "        Tier: MEM  Size: " + size + "\n"
        + "    Used Capacity: 0B\n"
        + "        Tier: MEM  Size: 0B\n"
        + "    Used Percentage: 0%\n"
        + "    Free Percentage: 100%\n"));
    Assert.assertThat(output, CoreMatchers.containsString(
        "Worker Name       Type          Total            MEM           SSD           HDD"));
    Assert.assertThat(output, CoreMatchers.containsString(
        "                  Used          0B (0%)          0B            0B            0B"));
  }

  @Test
  public void tooManyOptions() {
    int ret = mFsAdminShell.run("report", "capacity", "-live", "-lost");
    Assert.assertEquals(-1, ret);
    String expected = CapacityCommand.getUsage()
        + "\nToo many arguments passed in.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void capacityWithInvalidAddress() {
    int ret = mFsAdminShell.run("report", "capacity", "-worker", "0.0.0.0");
    Assert.assertEquals(-1, ret);
    Assert.assertThat(mOutput.toString(), CoreMatchers.containsString(CapacityCommand.getUsage()));
  }
}
