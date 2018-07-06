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

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for report metrics command.
 */
public final class MetricsCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void metrics() {
    int ret = mFsAdminShell.run("report", "metrics");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();
    checkMetricsResults(output);
  }

  /**
   * Checks if the metrics command output is valid.
   *
   * @param output the metrics command output
   */
  private void checkMetricsResults(String output) {
    List<String> expectedOutput = Arrays.asList(
        "Total IO Size: ",
        "    Short-circuit Read                               0B",
        "    From Remote Instances                            0B",
        "    Under Filesystem Read                            0B",
        "    Alluxio Write                                    0B",
        "    Under Filesystem Write                           0B",
        "",
        "Total IO Throughput (Last Minute): ",
        "    Short-circuit Read                               0B",
        "    From Remote Instances                            0B",
        "    Under Filesystem Read                            0B",
        "    Alluxio Write                                    0B",
        "    Under Filesystem Write                           0B",
        "",
        "Cache Hit Rate (Percentage): ",
        "    Alluxio Local                                  0.00",
        "    Alluxio Remote                                 0.00",
        "    Miss                                           0.00",
        "",
        "Logical Operations: ",
        "    Directories Created                               0",
        "    File Block Infos Got                              0",
        "    File Infos Got                                    1",
        "    Files Completed                                   0",
        "    Files Created                                     0",
        "    Files Freed                                       0",
        "    Files Persisted                                   0",
        "    New Blocks Got                                    0",
        "    Paths Deleted                                     0",
        "    Paths Mounted                                     0",
        "    Paths Renamed                                     0",
        "    Paths Unmounted                                   0",
        "",
        "RPC Invocations: ",
        "    Complete File Operations                          0",
        "    Create Directory Operations                       0",
        "    Create File Operations                            0",
        "    Delete Path Operations                            0",
        "    Free File Operations                              0",
        "    Get File Block Info Operations                    0",
        "    Get File Info Operations                          1",
        "    Get New Block Operations                          0",
        "    Mount Operations                                  0",
        "    Rename Path Operations                            0",
        "    Set Attribute Operations                          0",
        "    Unmount Operations                                0",
        "",
        "Other metrics information: ");
    List<String> testOutput = Arrays.asList(output.split("\n")).subList(0, expectedOutput.size());
    Assert.assertThat(testOutput,
        IsIterableContainingInOrder.contains(expectedOutput.toArray()));
  }
}
