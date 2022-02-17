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

package alluxio.stress;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import alluxio.stress.cli.BatchTaskRunner;
import alluxio.stress.common.FileSystemClientType;
import alluxio.stress.master.MasterBenchSummary;
import alluxio.util.JsonSerializable;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

public class BatchTaskRunnerIntegrationTest extends AbstractStressBenchIntegrationTest {
  @Test
  public void MasterIntegrationFileTest() throws Exception {
    // redirect the output stream
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(out));

    BatchTaskRunner.main(new String[]{
        "MasterComprehensiveFileBatchTask",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--warmup", "0s",
        "--threads", "1",
        "--stop-count", "100",
        "--write-type", "MUST_CACHE",
        "--read-type", "NO_CACHE",
        "--create-file-size", "0",
        "--client-type", "AlluxioHDFS",
        "--clients", "1",
        "--in-process",
    });

    String printOutResult = out.toString();
    List<String> resultList = getJsonResult(printOutResult);
    assertEquals(resultList.size(), 7);

    // the possible operations
    List<String> operation = ImmutableList.of("CreateFile", "ListDir", "ListDirLocated",
        "GetBlockLocations", "GetFileStatus", "OpenFile", "DeleteFile");
    for (int i = 0; i < resultList.size(); i++) {
      MasterBenchSummary summary = (MasterBenchSummary) JsonSerializable.fromJson(
          resultList.get(i));
      // confirm that the task was executed with certain parameter and output no errors
      assertEquals(summary.getParameters().mOperation.toString(), operation.get(i));
      assertEquals(summary.getParameters().mWarmup, "0s");
      assertEquals(summary.getParameters().mThreads, 1);
      assertEquals(summary.getParameters().mStopCount, 100);
      assertEquals(summary.getParameters().mWriteType, "MUST_CACHE");
      assertEquals(summary.getParameters().mReadType.toString(), "NO_CACHE");
      assertEquals(summary.getParameters().mCreateFileSize, "0");
      assertEquals(summary.getParameters().mClientType, FileSystemClientType.ALLUXIO_HDFS);
      assertEquals(summary.getParameters().mClients, 1);
      assertFalse(summary.getNodeResults().isEmpty());
      assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
    }

    // reset the output to the console
    System.setOut(originalOut);
  }
}
