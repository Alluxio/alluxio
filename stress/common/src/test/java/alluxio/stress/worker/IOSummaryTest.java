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

import java.util.HashSet;
import java.util.Set;

public class IOSummaryTest {
  @Test
  public void json() throws Exception {
    IOTaskResult result = new IOTaskResult();
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.READ, 100L, 20));
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.WRITE, 100L, 5));
    IOTaskSummary summary = new IOTaskSummary(result);

    // params
    BaseParameters baseParams = new BaseParameters();
    baseParams.mCluster = true;
    baseParams.mDistributed = true;
    baseParams.mId = "mock-id";
    baseParams.mStartMs = 0L;
    baseParams.mInProcess = false;
    summary.setBaseParameters(baseParams);

    WorkerBenchParameters workerParams = new WorkerBenchParameters();
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
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.READ, 100L, 20));
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.WRITE, 100L, 5));
    IOTaskSummary summary = new IOTaskSummary(result);
    IOTaskSummary.SpeedStat stat = summary.getReadSpeedStat();
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(stat);
    IOTaskSummary.SpeedStat other = mapper.readValue(json, IOTaskSummary.SpeedStat.class);
    checkEquality(stat, other);
  }

  private void checkEquality(IOTaskSummary.SpeedStat a, IOTaskSummary.SpeedStat b) {
    double delta = 1e-5;
    assertEquals(a.mTotalDuration, b.mTotalDuration, delta);
    assertEquals(a.mAvgSpeed, b.mAvgSpeed, delta);
    assertEquals(a.mMaxSpeed, b.mMaxSpeed, delta);
    assertEquals(a.mMinSpeed, b.mMinSpeed, delta);
    assertEquals(a.mStdDev, b.mStdDev, delta);
    assertEquals(a.mTotalSize, b.mTotalSize);
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
