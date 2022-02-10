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

import alluxio.stress.cli.RegisterWorkerBench;
import alluxio.stress.cli.WorkerHeartbeatBench;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.cli.GetPinnedFileIdsBench;
import alluxio.stress.cli.fuse.FuseIOBench;
import alluxio.stress.cli.StreamRegisterWorkerBench;
import alluxio.stress.cli.StressJobServiceBench;
import alluxio.stress.cli.UfsIOBench;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

public class BenchmarkUnitTest {
  @Test
  public void parseWriteTypeTest() {
    // confirm that the unsupported Benchmark will return an empty list.
    List<Benchmark> unsupportedBench = new ImmutableList.Builder<Benchmark>().add(
        new GetPinnedFileIdsBench(),
        new FuseIOBench(),
        new RegisterWorkerBench(),
        new StreamRegisterWorkerBench(),
        new StressJobServiceBench(),
        new UfsIOBench(),
        new WorkerHeartbeatBench(),
        new RegisterWorkerBench()).build();

    for (Benchmark bench : unsupportedBench) {
      assertTrue(bench.parseWriteTypes().isEmpty());
    }
  }
}
