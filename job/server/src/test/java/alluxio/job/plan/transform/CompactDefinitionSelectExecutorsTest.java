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

package alluxio.job.plan.transform;

import alluxio.job.plan.SelectExecutorsTest;

import org.junit.Test;

public class CompactDefinitionSelectExecutorsTest {

  private static String INPUT_DIR = "/input";
  private static String OUTPUT_DIR = "/output";

  @Test
  public void testExecutorsParallel() throws Exception {
    CompactConfig config = new CompactConfig(null, INPUT_DIR, OUTPUT_DIR, null, 100);
    new CompactDefinition().selectExecutors(config, SelectExecutorsTest.JOB_WORKERS)
  }

}
