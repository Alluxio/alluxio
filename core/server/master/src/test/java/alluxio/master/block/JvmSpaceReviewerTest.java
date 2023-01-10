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

package alluxio.master.block;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.grpc.GetRegisterLeasePRequest;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import java.util.SortedMap;
import java.util.TreeMap;

public class JvmSpaceReviewerTest {
  private static final long WORKER_ID = 1L;

  @Test
  public void reviewJvmUsage() {
    long usedBytes = 0L;
    long maxBytes = 1000L;

    // Mock the gauges
    MetricRegistry mockRegistry = mock(MetricRegistry.class);
    SortedMap<String, Gauge> heapGauges = new TreeMap<>();
    Gauge<Long> usedSpaceGauge = mock(Gauge.class);
    when(usedSpaceGauge.getValue()).thenReturn(usedBytes);
    Gauge<Long> maxSpaceGauge = mock(Gauge.class);
    when(maxSpaceGauge.getValue()).thenReturn(maxBytes);
    heapGauges.put("heap.used", usedSpaceGauge);
    heapGauges.put("heap.max", maxSpaceGauge);
    when(mockRegistry.getGauges(any())).thenReturn(heapGauges);

    Runtime mockRuntime = mock(Runtime.class);
    // max - (total - free) = 1000L
    when(mockRuntime.maxMemory()).thenReturn(1200L);
    when(mockRuntime.freeMemory()).thenReturn(0L);
    when(mockRuntime.totalMemory()).thenReturn(200L);

    JvmSpaceReviewer reviewer = new JvmSpaceReviewer(mockRuntime);

    // Determine how many blocks will be accepted
    long maxBlocks = (maxBytes - usedBytes) / JvmSpaceReviewer.BLOCK_COUNT_MULTIPLIER;

    GetRegisterLeasePRequest noBlocks = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER_ID).setBlockCount(0).build();
    assertTrue(reviewer.reviewLeaseRequest(noBlocks));

    GetRegisterLeasePRequest adequateBlocks = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER_ID).setBlockCount(maxBlocks).build();
    assertTrue(reviewer.reviewLeaseRequest(adequateBlocks));

    GetRegisterLeasePRequest tooManyBlocks = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER_ID).setBlockCount(maxBlocks + 1).build();
    assertFalse(reviewer.reviewLeaseRequest(tooManyBlocks));
  }
}
