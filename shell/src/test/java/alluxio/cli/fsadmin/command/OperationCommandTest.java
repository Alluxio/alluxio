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

import alluxio.cli.fsadmin.report.OperationCommand;
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

public class OperationCommandTest {
  private FileSystemMasterClient mFileSystemMasterClient;
  private ByteArrayOutputStream mOutputStream;
  private PrintStream mPrintStream;

  @Before
  public void prepareDependencies() throws IOException {
    // Generate random values for OperationInfo and rpcInvocationInfo
    Map<String, Long> operationInfo = generateOperationInfo();
    Map<String, Long> rpcInvocationInfo = generateRpcInvocationInfo();

    // Prepare mock file system master client
    mFileSystemMasterClient = Mockito.mock(FileSystemMasterClient.class);
    Mockito.when(mFileSystemMasterClient.getOperationInfo()).thenReturn(operationInfo);
    Mockito.when(mFileSystemMasterClient.getRpcInvocationInfo()).thenReturn(rpcInvocationInfo);

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
    OperationCommand operationCommand = new OperationCommand(mFileSystemMasterClient, mPrintStream);
    operationCommand.run();
    checkIfOutputValid();
  }

  /**
   * @return a generated operation info map
   */
  private Map<String, Long> generateOperationInfo() {
    Map<String, Long> map = new HashMap<>();
    map.put("FileInfosGot", 121L);
    map.put("CommandTest", 31243412L);
    map.put("AlluxioOperation", 12L);
    map.put("FilePinned", 0L);
    map.put("UfsSessionCount-Ufs:/alluxio", 5312L);
    return map;
  }

  /**
   * @return a generated rpc invocation info map
   */
  private Map<String, Long> generateRpcInvocationInfo() {
    Map<String, Long> map = new HashMap<>();
    map.put("MountOps", 534L);
    map.put("FileCreatedOps", 2141L);
    map.put("UnmountOps", 3123124L);
    return map;
  }

  /**
   * Checks if the output is expected.
   */
  private void checkIfOutputValid() {
    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    List<String> expectedOutput = Arrays.asList("Alluxio logical operations: ",
        "    Alluxio Operation                      12",
        "    Command Test                   31,243,412",
        "    File Infos Got                        121",
        "    File Pinned                             0",
        "",
        "Alluxio RPC invocations: ",
        "    File Created Operations             2,141",
        "    Mount Operations                      534",
        "    Unmount Operations              3,123,124");
    List<String> testOutput = Arrays.asList(output.split("\n"));
    Assert.assertThat(testOutput,
        IsIterableContainingInOrder.contains(expectedOutput.toArray()));
  }
}
