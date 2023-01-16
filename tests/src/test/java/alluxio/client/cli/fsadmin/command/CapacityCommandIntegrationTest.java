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

import alluxio.cli.fsadmin.report.CapacityCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.util.FormatUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

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
    String[] lines = output.split("\n");
    Assert.assertEquals(11, lines.length);
    Assert.assertEquals("Capacity information for all workers: ", lines[0]);
    Assert.assertEquals("    Total Capacity: " + size, lines[1]);
    Assert.assertEquals("        Tier: MEM  Size: " + size, lines[2]);
    Assert.assertEquals("    Used Capacity: 0B", lines[3]);
    Assert.assertEquals("        Tier: MEM  Size: 0B", lines[4]);
    Assert.assertEquals("    Used Percentage: 0%", lines[5]);
    Assert.assertEquals("    Free Percentage: 100%", lines[6]);
    Assert.assertEquals("", lines[7]);
    Assert.assertTrue(Pattern.matches(
        "Worker Name {6,}Last Heartbeat {3}Storage {7}MEM {14}Version {10}Revision *", lines[8]));
    Assert.assertTrue(lines[9].contains("capacity      " + size));
    Assert.assertTrue(lines[10].contains("used          0B (0%)"));
  }

  @Test
  public void lostCapacity() {
    int ret = mFsAdminShell.run("report", "capacity", "-lost");
    Assert.assertEquals(0, ret);
    Assert.assertEquals(mOutput.toString(), "No workers found.\n");
  }

  @Test
  public void liveCapacity() {
    int ret = mFsAdminShell.run("report", "capacity", "-live");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();
    String size = FormatUtils.getSizeFromBytes(SIZE_BYTES);
    String[] lines = output.split("\n");
    Assert.assertEquals(11, lines.length);
    Assert.assertEquals("Capacity information for live workers: ", lines[0]);
    Assert.assertEquals("    Total Capacity: " + size, lines[1]);
    Assert.assertEquals("        Tier: MEM  Size: " + size, lines[2]);
    Assert.assertEquals("    Used Capacity: 0B", lines[3]);
    Assert.assertEquals("        Tier: MEM  Size: 0B", lines[4]);
    Assert.assertEquals("    Used Percentage: 0%", lines[5]);
    Assert.assertEquals("    Free Percentage: 100%", lines[6]);
    Assert.assertEquals("", lines[7]);
    Assert.assertTrue(Pattern.matches(
        "Worker Name {6,}Last Heartbeat {3}Storage {7}MEM {14}Version {10}Revision *", lines[8]));
    Assert.assertTrue(lines[9].contains("capacity      " + size));
    Assert.assertTrue(lines[10].contains("used          0B (0%)"));
  }

  @Test
  public void tooManyOptions() {
    mFsAdminShell.run("report", "capacity", "-live", "-lost");
    String expected = CapacityCommand.getUsage()
        + "\nToo many arguments passed in.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
