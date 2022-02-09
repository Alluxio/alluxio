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

import alluxio.stress.cli.StressMasterBench;
import alluxio.stress.master.MasterBenchSummary;
import alluxio.util.JsonSerializable;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

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
    assertTrue(summary.getParameters().mOperation.toString().equals(operation));
    assertTrue(summary.getParameters().mBasePath.equals(basePath));
    assertTrue(summary.getParameters().mStopCount == 100);
    assertTrue(summary.getParameters().mTargetThroughput == 1000);
    assertTrue(summary.getParameters().mThreads == 5);
    assertTrue(summary.getParameters().mWarmup.equals("0s"));
    assertTrue(summary.getParameters().mDuration.equals("10s"));

    assertTrue(summary.getEndTimeMs() > startTime);
    assertTrue(summary.getNodeResults().size() >= 1);
    assertTrue(summary.getDurationMs() > 0);
    assertTrue(summary.getThroughput() > 0);
    assertTrue(summary.getStatistics().mNumSuccess == 100);
    assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
  }
}
