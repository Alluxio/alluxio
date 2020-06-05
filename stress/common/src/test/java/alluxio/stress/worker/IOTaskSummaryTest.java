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

package alluxio.stress.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.stress.BaseParameters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class IOTaskSummaryTest {
  @Test
  public void json() throws Exception {
    IOTaskResult result = new IOTaskResult();
    // Reading 200MB took 1s
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.READ, 1L, 200 * 1024 * 1024));
    // Writing 50MB took 1s
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.WRITE, 1L, 50 * 1024 * 1024));
    IOTaskSummary summary = new IOTaskSummary(result);

    // params
    BaseParameters baseParams = new BaseParameters();
    baseParams.mCluster = true;
    baseParams.mDistributed = true;
    baseParams.mId = "mock-id";
    baseParams.mStartMs = 0L;
    baseParams.mInProcess = false;
    summary.setBaseParameters(baseParams);

    UfsIOParameters workerParams = new UfsIOParameters();
    workerParams.mPath = "hdfs://path";
    summary.setParameters(workerParams);

    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(summary);

    IOTaskSummary other = mapper.readValue(json, IOTaskSummary.class);
    checkEquality(summary, other);
  }

  @Test
  public void statJson() throws Exception {
    IOTaskResult result = new IOTaskResult();
    // Reading 200MB took 1s
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.READ, 1L, 200 * 1024 * 1024));
    // Reading 196MB took 1s
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.READ, 1L, 196 * 1024 * 1024));
    IOTaskSummary summary = new IOTaskSummary(result);
    IOTaskSummary.SpeedStat stat = summary.getReadSpeedStat();
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(stat);
    IOTaskSummary.SpeedStat other = mapper.readValue(json, IOTaskSummary.SpeedStat.class);
    checkEquality(stat, other);
  }

  @Test
  public void statCalculation() {
    IOTaskResult result = new IOTaskResult();
    double[] durations = new double[]{1.0, 1.5, 2.0, 1.11};
    long[] sizes = new long[]{200_000_000, 300_000_000, 500_000_000, 800_000_000};
    for (int i = 0; i < sizes.length; i++) {
      result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.READ, durations[i], sizes[i]));
      result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.WRITE, durations[i], sizes[i]));
    }

    IOTaskSummary summary = new IOTaskSummary(result);
    IOTaskSummary.SpeedStat readStat = summary.getReadSpeedStat();
    double totalDuration = Arrays.stream(durations).sum();
    long totalSize = Arrays.stream(sizes).sum();
    double avgSpeed = totalSize / (totalDuration * 1024 * 1024);
    double maxSpeed = 800_000_000 / (1.11 * 1024 * 1024);
    double minSpeed = 200_000_000 / (1.0 * 1024 * 1024);
    assertEquals(totalDuration, readStat.mTotalDurationSeconds, 1e-5);
    assertEquals(totalSize, readStat.mTotalSizeBytes);
    assertEquals(avgSpeed, readStat.mAvgSpeedMbps, 1e-5);
    assertEquals(maxSpeed, readStat.mMaxSpeedMbps, 1e-5);
    assertEquals(minSpeed, readStat.mMinSpeedMbps, 1e-5);

    IOTaskSummary.SpeedStat writeStat = summary.getWriteSpeedStat();
    assertEquals(totalDuration, writeStat.mTotalDurationSeconds, 1e-5);
    assertEquals(totalSize, writeStat.mTotalSizeBytes);
    assertEquals(avgSpeed, writeStat.mAvgSpeedMbps, 1e-5);
    assertEquals(maxSpeed, writeStat.mMaxSpeedMbps, 1e-5);
    assertEquals(minSpeed, writeStat.mMinSpeedMbps, 1e-5);
  }

  private void checkEquality(IOTaskSummary.SpeedStat a, IOTaskSummary.SpeedStat b) {
    double delta = 1e-5;
    assertEquals(a.mTotalDurationSeconds, b.mTotalDurationSeconds, delta);
    assertEquals(a.mAvgSpeedMbps, b.mAvgSpeedMbps, delta);
    assertEquals(a.mMaxSpeedMbps, b.mMaxSpeedMbps, delta);
    assertEquals(a.mMinSpeedMbps, b.mMinSpeedMbps, delta);
    assertEquals(a.mStdDev, b.mStdDev, delta);
    assertEquals(a.mTotalSizeBytes, b.mTotalSizeBytes);
  }

  private void checkEquality(IOTaskSummary a, IOTaskSummary b) {
    assertEquals(a.getPoints().size(), b.getPoints().size());
    Set<IOTaskResult.Point> points = new HashSet<>(a.getPoints());
    for (IOTaskResult.Point p : b.getPoints()) {
      assertTrue(points.contains(p));
    }
    assertEquals(a.getErrors().size(), b.getErrors().size());
    Set<String> errors = new HashSet<>(a.getErrors());
    for (String e : b.getErrors()) {
      assertTrue(errors.contains(e));
    }
  }
}
