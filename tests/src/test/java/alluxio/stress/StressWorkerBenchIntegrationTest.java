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

import alluxio.stress.cli.worker.StressWorkerBench;

import static org.junit.Assert.assertTrue;

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

    System.out.println(output);

    generateAndVerifyReport(Collections.singletonList("Worker Throughput"), output);
  }

  @Test
  public void testForMultipleNodeResults() throws Exception {
    String output = new StressWorkerBench().run(new String[] {
        "--in-process",
        "--start-ms", Long.toString(System.currentTimeMillis() + 2000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/stress-worker-base/",
        "--threads", "2",
        "--file-size", "1m",
        "--block-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    WorkerBenchSummary summary = (WorkerBenchSummary) JsonSerializable.fromJson(output);
    assertTrue(summary.getNodeResults().size() >= 1);
    assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
  }
}
