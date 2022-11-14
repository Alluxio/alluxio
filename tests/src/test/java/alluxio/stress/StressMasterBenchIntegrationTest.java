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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.stress.cli.StressMasterBench;
import alluxio.stress.master.MasterBenchSummary;
import alluxio.util.JsonSerializable;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link StressMasterBench}.
 */
public class StressMasterBenchIntegrationTest extends AbstractStressBenchIntegrationTest {
  @Test
  public void createFileAndDelete() throws Exception {
    // Only in-process will work for unit testing.
    String output = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "CreateFile",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });
    generateAndVerifyReport(Collections.singletonList("CreateFile"), output);

    // run again to test the deletion of the test directory
    output = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "CreateFile",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "2",
        "--warmup", "0s", "--duration", "1s",
    });

    String output2 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "GetBlockLocations",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });

    String output3 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "OpenFile",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });

    String output4 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "DeleteFile",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });

    generateAndVerifyReport(
        Arrays.asList("CreateFile", "GetBlockLocations", "OpenFile", "DeleteFile"), output, output2,
        output3, output4);
  }

  @Test
  public void createDir() throws Exception {
    // Only in-process will work for unit testing.
    String output = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "CreateDir",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });
    generateAndVerifyReport(Collections.singletonList("CreateDir"), output);
  }

  @Test
  public void createFileAndListAndRename() throws Exception {
    // Only in-process will work for unit testing.
    String output1 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "CreateFile",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });

    String output2 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "GetFileStatus",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });

    String output3 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "ListDir",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });

    String output4 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "ListDirLocated",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });

    String output5 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "RenameFile",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
    });

    generateAndVerifyReport(
        Arrays.asList("CreateFile", "GetFileStatus", "ListDir", "ListDirLocated", "RenameFile"),
        output1, output2, output3, output4, output5);
  }

  @Test
  public void writeTypeSingleTaskTest() throws Exception {
    String[] writeType = new String[] {"MUST_CACHE", "CACHE_THROUGH", "THROUGH", "ASYNC_THROUGH"};

    for (int i = 0; i < writeType.length; i++) {
      validateTheResultWithWriteType(writeType[i]);
    }
  }

  private void validateTheResultWithWriteType(String writeType) throws Exception {
    String output1 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "CreateFile",
        "--fixed-count", "20",
        "--target-throughput", "300",
        "--threads", "5",
        "--warmup", "0s", "--duration", "3s",
        "--write-type", writeType,
    });

    String output2 = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "DeleteFile",
        "--fixed-count", "20",
        "--target-throughput", "100",
        "--threads", "5",
        "--warmup", "0s", "--duration", "1s",
        "--write-type", writeType,
    });

    // convert the result into summary, and check whether it have errors.
    MasterBenchSummary summary1 = (MasterBenchSummary) JsonSerializable.fromJson(output1);
    MasterBenchSummary summary2 = (MasterBenchSummary) JsonSerializable.fromJson(output2);

    // confirm that the results contain information, and they don't contain errors.
    assertFalse(summary1.getNodeResults().isEmpty());
    assertTrue(summary1.collectErrorsFromAllNodes().isEmpty());
    assertFalse(summary2.getNodeResults().isEmpty());
    assertTrue(summary2.collectErrorsFromAllNodes().isEmpty());
  }

  @Test
  public void writeTypeALLTaskTest() throws Exception {
    // redirect the output stream
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(out));

    new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "CreateFile",
        "--fixed-count", "20",
        "--target-throughput", "300",
        "--threads", "5",
        "--warmup", "0s", "--duration", "3s",
        "--write-type", "ALL",
    });

    String printOutResult = out.toString();
    List<String> resultList = getJsonResult(printOutResult);

    assertEquals(resultList.size(), 4);

    // the possible write types
    List<String> writeTypes = ImmutableList.of("MUST_CACHE", "CACHE_THROUGH",
        "ASYNC_THROUGH", "THROUGH");
    for (int i = 0; i < resultList.size(); i++) {
      MasterBenchSummary summary = (MasterBenchSummary) JsonSerializable.fromJson(
          resultList.get(i));
      // confirm that the task was executed with certain write type and output no errors
      assertEquals(summary.getParameters().mWriteType, writeTypes.get(i));
      assertFalse(summary.getNodeResults().isEmpty());
      assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
    }

    // reset the output to the console
    System.setOut(originalOut);
  }

  @Test
  public void testForMultipleNodeResults() throws Exception {
    // The RenameFile will change the name of the created file, to avoid the DeleteFile
    // can't find file to delete, operate CreateFile twice
    String[] operations = {"CreateFile", "GetBlockLocations", "GetFileStatus", "OpenFile",
        "ListDir", "ListDirLocated", "RenameFile", "CreateFile", "DeleteFile", "CreateDir"};

    for (String op : operations) {
      validateTheOutput(op);
    }
  }

  private void validateTheOutput(String operation) throws Exception {
    long startTime = System.currentTimeMillis();
    String basePath = sLocalAlluxioClusterResource.get().getMasterURI() + "/";
    String output = new StressMasterBench().run(new String[] {
        "--in-process",
        "--base", basePath,
        "--operation", operation,
        "--stop-count", "100",
        "--target-throughput", "1000",
        "--threads", "5",
        "--warmup", "0s", "--duration", "10s",
    });

    MasterBenchSummary summary = (MasterBenchSummary) JsonSerializable.fromJson(output);
    assertEquals(summary.getParameters().mOperation.toString(), operation);
    assertEquals(summary.getParameters().mBasePath, basePath);
    assertEquals(summary.getParameters().mStopCount, 100);
    assertEquals(summary.getParameters().mTargetThroughput, 1000);
    assertEquals(summary.getParameters().mThreads, 5);
    assertEquals(summary.getParameters().mWarmup, "0s");
    assertEquals(summary.getParameters().mDuration, "10s");

    assertTrue(summary.getEndTimeMs() > startTime);
    assertTrue(summary.getNodeResults().size() >= 1);
    assertTrue(summary.getDurationMs() > 0);
    assertTrue(summary.getThroughput() > 0);
    //assertEquals(summary.getStatistics().mNumSuccess, 100);
    assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
  }
}
