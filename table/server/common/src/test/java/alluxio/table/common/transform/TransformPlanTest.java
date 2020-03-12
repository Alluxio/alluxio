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

package alluxio.table.common.transform;

import static org.junit.Assert.assertEquals;

import alluxio.job.JobConfig;
import alluxio.job.plan.transform.CompactConfig;
import alluxio.table.common.TableTestUtils;
import alluxio.table.common.layout.HiveLayout;

import org.junit.Test;

import java.util.ArrayList;

public class TransformPlanTest {
  @Test
  public void getJobConfigs() {
    HiveLayout from = TableTestUtils.createLayout("/from");
    HiveLayout to = TableTestUtils.createLayout("/to");
    TransformDefinition definition =
        TransformDefinition.parse("file.count.max=12");

    TransformPlan plan = new TransformPlan(from, to, definition);
    assertEquals(from, plan.getBaseLayout());
    assertEquals(to, plan.getTransformedLayout());
    ArrayList<JobConfig> jobs = plan.getJobConfigs();
    assertEquals(1, jobs.size());
    assertEquals(CompactConfig.class, jobs.get(0).getClass());

    CompactConfig compact = (CompactConfig) jobs.get(0);
    assertEquals("/from", compact.getInput());
    assertEquals("/to", compact.getOutput());
    assertEquals(12, compact.getMaxNumFiles());
  }
}
