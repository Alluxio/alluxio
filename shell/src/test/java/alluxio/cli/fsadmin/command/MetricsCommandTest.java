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

import alluxio.cli.fsadmin.report.MetricsCommand;
import alluxio.client.file.FileSystemMasterClient;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricsCommandTest {
  private FileSystemMasterClient mFileSystemMasterClient;
  private ByteArrayOutputStream mOutputStream;
  private PrintStream mPrintStream;

  @Before
  public void prepareDependencies() throws IOException {
    Map<String, Long> metricsMap = generateMetricsMap();

    // Prepare mock file system master client
    mFileSystemMasterClient = Mockito.mock(FileSystemMasterClient.class);
    Mockito.when(mFileSystemMasterClient.getMetrics()).thenReturn(metricsMap);

    // Prepare print stream
    mOutputStream = new ByteArrayOutputStream();
    mPrintStream = new PrintStream(mOutputStream, true, "utf-8");
  }

  @After
  public void after() {
    mPrintStream.close();
  }

  @Test
  public void operation() throws IOException {
    MetricsCommand metricsCommand = new MetricsCommand(mFileSystemMasterClient, mPrintStream);
    metricsCommand.run();
    checkIfOutputValid();
  }

  /**
   * @return a generated metrics map
   */
  private Map<String, Long> generateMetricsMap() {
    Map<String, Long> map = new HashMap<>();
    map.put("DirectoriesCreated", 121L);
    map.put("FileBlockInfosGot", 31243412L);
    map.put("FileInfosGot", 12L);
    map.put("FilesCompleted", 0L);
    map.put("FilesCreated", 534L);
    map.put("FilesFreed", 2141L);
    map.put("FilesPersisted", 4171L);
    map.put("master.FilesPinned", 2354239L);
    map.put("NewBlocksGot", 4L);
    map.put("PathsDeleted", 583L);
    map.put("PathsMounted", 3635L);
    map.put("PathsRenamed", 382L);
    map.put("PathsUnmounted", 975L);

    map.put("CompleteFileOps", 813L);
    map.put("CreateDirectoryOps", 325728397L);
    map.put("CreateFileOps", 89L);
    map.put("DeletePathOps", 21L);
    map.put("FreeFileOps", 5213L);
    map.put("GetFileBlockInfoOps", 798L);
    map.put("GetFileInfoOps", 32L);
    map.put("GetNewBlockOps", 912572136653L);
    map.put("MountOps", 953795L);
    map.put("RenamePathOps", 29L);
    map.put("SetAttributeOps", 0L);
    map.put("UnmountOps", 1L);

    map.put("UfsSessionCount-Ufs:/alluxio", 5312L);
    map.put("RandomGeneratedProperty", 212L);
    return map;
  }

  /**
   * Checks if the output is expected.
   */
  private void checkIfOutputValid() {
    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    List<String> expectedOutput = Arrays.asList("Alluxio logical operations: ",
        "    Directories Created                             121",
        "    File Block Infos Got                     31,243,412",
        "    File Infos Got                                   12",
        "    Files Completed                                   0",
        "    Files Created                                   534",
        "    Files Freed                                   2,141",
        "    Files Persisted                               4,171",
        "    Files Pinned                              2,354,239",
        "    New Blocks Got                                    4",
        "    Paths Deleted                                   583",
        "    Paths Mounted                                 3,635",
        "    Paths Renamed                                   382",
        "    Paths Unmounted                                 975",
        "",
        "Alluxio RPC invocations: ",
        "    Complete File Operations                        813",
        "    Create Directory Operations             325,728,397",
        "    Create File Operations                           89",
        "    Delete Path Operations                           21",
        "    Free File Operations                          5,213",
        "    Get File Block Info Operations                  798",
        "    Get File Info Operations                         32",
        "    Get New Block Operations            912,572,136,653",
        "    Mount Operations                            953,795",
        "    Rename Path Operations                           29",
        "    Set Attribute Operations                          0",
        "    Unmount Operations                                1");
    List<String> testOutput = Arrays.asList(output.split("\n"));
    Assert.assertThat(testOutput,
        IsIterableContainingInOrder.contains(expectedOutput.toArray()));
  }
}
