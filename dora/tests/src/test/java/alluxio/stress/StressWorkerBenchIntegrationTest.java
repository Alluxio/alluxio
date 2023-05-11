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
import static org.junit.Assert.assertTrue;

import alluxio.stress.cli.worker.StressWorkerBench;
import alluxio.stress.worker.WorkerBenchSummary;
import alluxio.util.JsonSerializable;

import org.junit.Test;

import java.util.Collections;

/**
 * Tests {@link StressWorkerBench}.
 */
public class StressWorkerBenchIntegrationTest extends AbstractStressBenchIntegrationTest {
  @Test
  public void readThroughput() throws Exception {
    // Only in-process will work for unit testing.
    String output = new StressWorkerBench().run(new String[] {
        "--in-process",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/stress-worker-base/",
        "--threads", "2",
        "--file-size", "1m",
        "--block-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    generateAndVerifyReport(Collections.singletonList("Worker Throughput"), output);
  }

  @Test
  public void testForMultipleNodeResults() throws Exception {
    long startTime = System.currentTimeMillis();
    String basePath = sLocalAlluxioClusterResource.get().getMasterURI() + "/stress-worker-base/";
    String output = new StressWorkerBench().run(new String[] {
        "--in-process",
        "--base", basePath,
        "--threads", "2",
        "--file-size", "1m",
        "--block-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    WorkerBenchSummary summary = (WorkerBenchSummary) JsonSerializable.fromJson(output);
    assertEquals(summary.getParameters().mBasePath, basePath);
    assertEquals(summary.getParameters().mFileSize, "1m");
    assertEquals(summary.getParameters().mThreads, 2);
    assertEquals(summary.getParameters().mBlockSize, "128k");
    assertEquals(summary.getParameters().mWarmup, "0s");
    assertEquals(summary.getParameters().mDuration, "1s");

    assertTrue(summary.getEndTimeMs() > startTime);
    assertTrue(summary.getIOBytes() > 0);
    assertTrue(summary.getDurationMs() > 0);
    assertTrue(summary.getNodeResults().size() >= 1);
    assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
  }
}
