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
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for report metrics command.
 */
public final class MetricsCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  @Ignore("ALLUXIO-3226")
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
    List<String> expectedOutput = Arrays.asList("Alluxio logical operations: ",
        "    DirectoriesCreated                           0",
        "    FileBlockInfosGot                            0",
        "    FileInfosGot                                 1",
        "    FilesCompleted                               0",
        "    FilesCreated                                 0",
        "    FilesFreed                                   0",
        "    FilesPersisted                               0",
        "    NewBlocksGot                                 0",
        "    PathsDeleted                                 0",
        "    PathsMounted                                 0",
        "    PathsRenamed                                 0",
        "    PathsUnmounted                               0",
        "",
        "Alluxio RPC invocations: ",
        "    CompleteFileOps                              0",
        "    CreateDirectoryOps                           0",
        "    CreateFileOps                                0",
        "    DeletePathOps                                0",
        "    FreeFileOps                                  0",
        "    GetFileBlockInfoOps                          0",
        "    GetFileInfoOps                               1",
        "    GetNewBlockOps                               0",
        "    MountOps                                     0",
        "    RenamePathOps                                0",
        "    SetAttributeOps                              0",
        "    UnmountOps                                   0",
        "",
        "Other metrics information: ");
    List<String> testOutput = Arrays.asList(output.split("\n")).subList(0, expectedOutput.size());
    Assert.assertThat(testOutput,
        IsIterableContainingInOrder.contains(expectedOutput.toArray()));
  }
}
