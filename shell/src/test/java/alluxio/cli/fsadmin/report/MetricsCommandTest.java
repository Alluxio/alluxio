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

package alluxio.cli.fsadmin.report;

import alluxio.cli.fsadmin.report.MetricsCommand;
import alluxio.client.MetaMasterClient;
import alluxio.wire.MetricValue;

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
  private MetaMasterClient mMetaMasterClient;
  private ByteArrayOutputStream mOutputStream;
  private PrintStream mPrintStream;

  @Before
  public void before() throws IOException {
    Map<String, MetricValue> metricsMap = generateMetricsMap();

    // Prepare mock meta master client
    mMetaMasterClient = Mockito.mock(MetaMasterClient.class);
    Mockito.when(mMetaMasterClient.getMetrics()).thenReturn(metricsMap);

    // Prepare print stream
    mOutputStream = new ByteArrayOutputStream();
    mPrintStream = new PrintStream(mOutputStream, true, "utf-8");
  }

  @After
  public void after() {
    mPrintStream.close();
  }

  @Test
  public void metrics() throws IOException {
    MetricsCommand metricsCommand = new MetricsCommand(mMetaMasterClient, mPrintStream);
    metricsCommand.run();
    checkIfOutputValid();
  }

  /**
   * @return a generated metrics map
   */
  private Map<String, MetricValue> generateMetricsMap() {
    Map<String, MetricValue> map = new HashMap<>();
    map.put("DirectoriesCreated", MetricValue.forLong(121L));
    map.put("FileBlockInfosGot", MetricValue.forLong(31243412L));
    map.put("FileInfosGot", MetricValue.forLong(12312321434276L));
    map.put("FilesCompleted", MetricValue.forLong(0L));
    map.put("FilesCreated", MetricValue.forLong(534L));
    map.put("FilesFreed", MetricValue.forLong(2141L));
    map.put("FilesPersisted", MetricValue.forLong(4171L));
    map.put("master.FilesPinned", MetricValue.forLong(2354239L));
    map.put("NewBlocksGot", MetricValue.forLong(4L));
    map.put("PathsDeleted", MetricValue.forLong(583L));
    map.put("PathsMounted", MetricValue.forLong(3635L));
    map.put("PathsRenamed", MetricValue.forLong(382L));
    map.put("PathsUnmounted", MetricValue.forLong(975L));

    map.put("CompleteFileOps", MetricValue.forLong(813L));
    map.put("CreateDirectoryOps", MetricValue.forLong(325728397L));
    map.put("CreateFileOps", MetricValue.forLong(89L));
    map.put("DeletePathOps", MetricValue.forLong(21L));
    map.put("FreeFileOps", MetricValue.forLong(5213L));
    map.put("GetFileBlockInfoOps", MetricValue.forLong(798L));
    map.put("GetFileInfoOps", MetricValue.forLong(32L));
    map.put("GetNewBlockOps", MetricValue.forLong(912572136653L));
    map.put("MountOps", MetricValue.forLong(953795L));
    map.put("RenamePathOps", MetricValue.forLong(29L));
    map.put("SetAttributeOps", MetricValue.forLong(0L));
    map.put("UnmountOps", MetricValue.forLong(1L));

    map.put("UfsSessionCount-Ufs:_alluxio_underFSStorage",
        MetricValue.forLong(8535L));
    map.put("UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder",
        MetricValue.forLong(1231L));
    map.put("master.CapacityTotal", MetricValue.forLong(1154531246129122L));
    map.put("heap.used", MetricValue.forDouble(0.0028321312));
    map.put("pools.Metaspace.usage", MetricValue.forDouble(0.95728));
    map.put("pools.Metaspace.max", MetricValue.forLong(-1));
    return map;
  }

  /**
   * Checks if the output is expected.
   */
  private void checkIfOutputValid() {
    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    // CHECKSTYLE.OFF: LineLengthExceed - Much more readable
    List<String> expectedOutput = Arrays.asList("Alluxio logical operations: ",
        "    DirectoriesCreated                         121",
        "    FileBlockInfosGot                   31,243,412",
        "    FileInfosGot                12,312,321,434,276",
        "    FilesCompleted                               0",
        "    FilesCreated                               534",
        "    FilesFreed                               2,141",
        "    FilesPersisted                           4,171",
        "    FilesPinned                          2,354,239",
        "    NewBlocksGot                                 4",
        "    PathsDeleted                               583",
        "    PathsMounted                             3,635",
        "    PathsRenamed                               382",
        "    PathsUnmounted                             975",
        "",
        "Alluxio RPC invocations: ",
        "    CompleteFileOps                            813",
        "    CreateDirectoryOps                 325,728,397",
        "    CreateFileOps                               89",
        "    DeletePathOps                               21",
        "    FreeFileOps                              5,213",
        "    GetFileBlockInfoOps                        798",
        "    GetFileInfoOps                              32",
        "    GetNewBlockOps                 912,572,136,653",
        "    MountOps                               953,795",
        "    RenamePathOps                               29",
        "    SetAttributeOps                              0",
        "    UnmountOps                                   1",
        "",
        "Other metrics information: ",
        "    UfsSessionCount-Ufs:_alluxio_underFSStorage  (8,535)",
        "    UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder  (1,231)",
        "    heap.used  (0.00283)",
        "    master.CapacityTotal  (1,154,531,246,129,122)",
        "    pools.Metaspace.max  (-1)",
        "    pools.Metaspace.usage  (0.95728)");
    // CHECKSTYLE.ON: LineLengthExceed
    List<String> testOutput = Arrays.asList(output.split("\n"));
    Assert.assertThat(testOutput,
        IsIterableContainingInOrder.contains(expectedOutput.toArray()));
  }
}
