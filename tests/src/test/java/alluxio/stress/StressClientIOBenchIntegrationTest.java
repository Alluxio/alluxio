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

import alluxio.stress.cli.client.StressClientIOBench;

import org.junit.Test;

import java.util.Arrays;

/**
 * Tests {@link StressClientIOBench}.
 */
public class StressClientIOBenchIntegrationTest extends AbstractStressBenchIntegrationTest {
  @Test
  public void readIO() throws Exception {
    // All the reads are in the same test, to re-use write results.
    // Only in-process will work for unit testing.

    String output1 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--start-ms", Long.toString(System.currentTimeMillis() + 5000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "WRITE",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output2 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--tag", "READ_ARRAY-NOT_RANDOM",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "READ_ARRAY",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output3 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "READ_ARRAY-RANDOM",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "READ_ARRAY",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output4 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "READ_BYTE_BUFFER",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "READ_BYTE_BUFFER",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output5 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "READ_FULLY",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "READ_FULLY",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output6 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "POS_READ-test",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "POS_READ",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output7 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "POS_READ_FULLY",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "POS_READ_FULLY",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });
    generateAndVerifyReport(Arrays.asList(
        "WRITE", "READ_ARRAY-NOT_RANDOM", "READ_ARRAY-RANDOM", "READ_BYTE_BUFFER", "READ_FULLY",
        "POS_READ-test", "POS_READ_FULLY"),
        output1, output2, output3, output4, output5, output6, output7);
  }
}
