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
  public void readArray() throws Exception {
    // Only in-process will work for unit testing.
    String output1 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--tag", "ReadArray-NOT_RANDOM",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "ReadArray",
        "--threads", "2",
        "--file-size", "1m",
        "--block-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output2 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "ReadArray-RANDOM",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "ReadArray",
        "--threads", "2",
        "--file-size", "1m",
        "--block-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });
    generateAndVerifyReport(Arrays.asList("ReadArray-NOT_RANDOM", "ReadArray-RANDOM"), output1,
        output2);
  }

  @Test
  public void posRead() throws Exception {
    // Only in-process will work for unit testing.
    String output1 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--tag", "PosRead-NOT_RANDOM",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "PosRead",
        "--threads", "2",
        "--file-size", "1m",
        "--block-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output2 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "PosRead-RANDOM",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--operation", "PosRead",
        "--threads", "2",
        "--file-size", "1m",
        "--block-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });
    generateAndVerifyReport(Arrays.asList("PosRead-NOT_RANDOM", "PosRead-RANDOM"), output1,
        output2);
  }
}
