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

import alluxio.exception.ExceptionMessage;
import alluxio.job.JobConfig;
import alluxio.job.plan.transform.CompactConfig;
import alluxio.table.common.TableTestUtils;
import alluxio.table.common.layout.HiveLayout;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class WriteActionTest {
  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Test
  public void noArgs() {
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage(ExceptionMessage.TRANSFORM_WRITE_ACTION_INVALID_ARGS.toString());
    TransformAction.Parser.parse("write()");
  }

  @Test
  public void tooManyArgs() {
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage(ExceptionMessage.TRANSFORM_WRITE_ACTION_INVALID_ARGS.toString());
    TransformAction.Parser.parse("write(one,two)");
  }

  @Test
  public void negativeNumFiles() {
    String option = "option(hive.num.files, -1)";
    String definition = "write(hive)." + option;
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage(ExceptionMessage.TRANSFORM_ACTION_PARSE_FAILED.getMessage(option));
    TransformAction.Parser.parse(definition);
  }

  @Test
  public void zeroNumFiles() {
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage(ExceptionMessage.TRANSFORM_WRITE_ACTION_INVALID_NUM_FILES.toString());
    TransformAction.Parser.parse("write(hive).option(hive.num.files, 0)");
  }

  @Test
  public void generateJobConfig() {
    TransformAction action = TransformAction.Parser.parse("write(hive).option(hive.num.files, 12)");
    assertEquals(WriteAction.class, action.getClass());
    WriteAction writeAction = (WriteAction) action;

    HiveLayout from = TableTestUtils.createLayout("/from");
    HiveLayout to = TableTestUtils.createLayout("/to");
    JobConfig job = writeAction.generateJobConfig(from, to);
    assertEquals(CompactConfig.class, job.getClass());

    CompactConfig compact = (CompactConfig) job;
    assertEquals("hive", compact.getDatabaseType());
    assertEquals("/from", compact.getInput());
    assertEquals("/to", compact.getOutput());
    assertEquals(12, compact.getNumFiles());
  }
}
