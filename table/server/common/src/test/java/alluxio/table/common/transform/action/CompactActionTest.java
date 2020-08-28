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

package alluxio.table.common.transform.action;

import static org.junit.Assert.assertEquals;

import alluxio.job.JobConfig;
import alluxio.job.plan.transform.CompactConfig;
import alluxio.table.common.TableTestUtils;
import alluxio.table.common.layout.HiveLayout;
import alluxio.table.common.transform.TransformDefinition;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class CompactActionTest {
  @Rule
  public ExpectedException mException = ExpectedException.none();

  public CompactAction parse(String definition) {
    TransformDefinition transformDefinition = TransformDefinition.parse(definition);

    final List<TransformAction> actions = transformDefinition.getActions();
    assertEquals(1, actions.size());
    final TransformAction action = actions.get(0);

    assertEquals(CompactAction.class, action.getClass());
    return (CompactAction) action;
  }

  @Test
  public void invalidNumFiles() {
    String definition = "file.count.max=0";
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage("Write action must have positive number of files");

    parse(definition);
  }

  @Test
  public void dynamicNumFiles() {
    final CompactAction compactAction = parse("file.count.max=1000;file.size.min=1024");

    HiveLayout from = TableTestUtils.createLayout("/from");
    HiveLayout to = TableTestUtils.createLayout("/to");
    JobConfig job = compactAction.generateJobConfig(from, to, false);
    assertEquals(CompactConfig.class, job.getClass());

    CompactConfig compact = (CompactConfig) job;
    assertEquals("/from", compact.getInput());
    assertEquals("/to", compact.getOutput());
    assertEquals(1000, compact.getMaxNumFiles());
    assertEquals(1024, compact.getMinFileSize());
  }

  @Test
  public void generateJobConfig() {
    final CompactAction compactAction = parse("file.count.max=12");

    HiveLayout from = TableTestUtils.createLayout("/from");
    HiveLayout to = TableTestUtils.createLayout("/to");
    JobConfig job = compactAction.generateJobConfig(from, to, false);
    assertEquals(CompactConfig.class, job.getClass());

    CompactConfig compact = (CompactConfig) job;
    assertEquals("/from", compact.getInput());
    assertEquals("/to", compact.getOutput());
    assertEquals(12, compact.getMaxNumFiles());
  }
}
